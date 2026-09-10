# Issue #958 — Growatt Storage SemReg Cutover

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Branch: `issue/958-growatt-storage-semreg-cutover`
- Base commit: `32244901c4c8337266cd348bdad90fb9e8eb0a61`
- Base tree: `0a850d5646d46f5782b1396d72a3d93bffa6974e`
- Implementation HEAD: `df1d96b18bcb30fa6b92426c359d5aa762a21869`
- Implementation tree: `08a88a50950044b4defd056ff25dbdb2457aa4d3`
- Reachability correction HEAD: `a6e9139ace75d192c269930276867ee6bb0c56da`
- Reachability correction tree: `8ef1c1c0cea2287a44d57653cdb10e2e74db1f29`
- Dependency pins: SemReg `f3f761bc67e10d6a65eba6c13cb4dc51002d6955`; docs-semantic mapping `f830ace6c2b9dd1af0e87ce808fa545662578418`.

This report is committed immediately after the implementation commit so it can
record its exact immutable HEAD and tree without a self-reference. No remote
mutation was performed.

## Exact-HEAD review corrections

- Correction code commit: `b8f3fceed4cca4af23d72e4310a26cbd30356795`
- Correction code tree: `9a2984a9d2e95b3f1a38785aab6d0c87ea286e7d`
- Reviewed PR discussions: `discussion_r3974190402`,
  `discussion_r3974190408`, `discussion_r3974190413`, and
  `discussion_r3974190417`.

The semantic publisher has an asset-local publication cursor. It is independent
from the native observation revision and advances only when the staged kernel,
evaluation, projection, and public JSON have all succeeded. Native MCP reads,
failed native samples, and rejected duplicate publication batches therefore do
not create a semantic revision gap or replace public state. Concurrent semantic
reads produce exactly the contiguous revisions 1 through 8.

When a previously promoted active or standby operating state becomes
`soft_starting`, the batch withdraws its exact prior candidate before projecting
the explicit withheld disposition. The public snapshot has no operating fact,
the withheld disposition has no source keys, and the six unrelated accepted
facts remain. The state transition is covered for active and standby origins;
the serialized publication path also covers replay and concurrent reads.

Portal bootstrap publishes `semantic_storage_current` with the canonical BFF
path under both enabled and disabled capability states. GraphQL recognized
storage-operation contract, asset, quota, and source failures now use the
`semanticStorageCurrent` error path. Parsing and other pre-operation failures
use a stable empty path; PV retains `semanticPVCurrent`.

## Delivered behavior

`GrowattBMSRS485Config` now requires a distinct explicit non-secret `asset_id`
and `source_id`. Neither can be inferred from unit ID, revision, vendor data,
topology, or observation bytes.

One serialized native observation transaction produces both the typed status and
its four immutable FC03 receipts. Only the seven accepted storage facts are
published through a stateful per-asset `PublicationKernel`; `soft_starting` is
withheld, and transformed current, operating-state, and cumulative-capacity
facts declare their accepted losses. Rejection occurs before the staged kernel
replaces the current view.

The new `semantic.v1.storage.growatt.current.get` MCP read, the mTLS
`SemanticStorageCurrent` GraphQL operation, and the Portal
`/api/v1/semantic/storage/current` BFF all use the same detached SemReg view.
The native `modbus.v1.growatt.bms.rs485.status.get` contract is unchanged.

No storage write, limit, interlock, acknowledgement, readback, retry, control,
fallback, comparator, shadow authority, or dual semantic publication was added.
Physical qualification, device access, serial access, credentials, deployment,
and Home Assistant work remain outside this repository-local result.

## Reachability correction

Every GraphQL and Portal storage read now invokes the same serialized native
observation-to-SemReg publication transaction as semantic MCP. A valid first
GraphQL/Portal request is reachable without MCP priming. A native source failure
returns unavailable and preserves the last known good projection.

The integration test covers first Portal-through-GraphQL publication and source
failure retention. MCP `data_timestamp` is derived from the authoritative
`evaluation.context.evaluated_at`, never handler wall time. The transport and
passive-smoke gates now have finite line-by-line `PortalStorage` allowlists;
hostile tests prove an extra `HTTPAddr` line still triggers required evidence.

## RED/GREEN and validation

- RED: concurrent storage publication initially exposed SemReg
  `revision_conflict: object revision`; the focused race test captured it.
- GREEN: refresh batches now carry only newer fact revisions after the stable
  identity/binding topology is established. `TestGrowattBMSRS485SemanticStorageSerializesConcurrentObservations` passes under `-race`.
- Focused: `GOWORK=off go test -race ./cmd/gateway -run 'TestGrowattBMSRS485SemanticStorage' -count=1` — PASS.
- GraphQL/MCP/Portal focused packages: PASS.
- Mapping gate: `./scripts/growatt_storage_semreg_gate.sh` — mapping validator
  PASS (2 executable outputs, 13 rejected scenarios) and pinned SemReg storage
  tests PASS.
- Full local CI: `GOWORK=off ./scripts/ci_local.sh` — PASS, including race
  suite, Go vet/build, portal tests/assets, Python gate suites, lint, Modbus RTU
  conformance, source-specific mapping gate, and passive-smoke classification.
- Correction CI: `GOWORK=off ./scripts/ci_local.sh` — PASS after the
  reachability and gate-classifier fixes.
- Exact-HEAD correction focused race: `GOWORK=off go test -race
  ./cmd/gateway ./mcp ./m2mgraphql ./portal -run
  'TestGrowattStorage|TestGrowattBMSRS485SemanticStorage|TestSemanticStorage|TestPortalBootstrapPublishes'
  -count=1` — PASS.
- Hostile gate controls: `python3 scripts/transport_gate_test.py` (23 PASS)
  and `python3 scripts/passive_smoke_gate_test.py` (9 PASS); an allowed
  `PortalStorage`-only diff passes classification and an unrelated config line
  does not bypass its applicable gate.
- Exact-HEAD full CI: `GOWORK=off ./scripts/ci_local.sh` — PASS: 168 Python
  tests, six additional Python suites (6/23/9/6/2), lint with 0 issues, all Go
  race packages, transport, mapping+SemReg, and passive-smoke gates.

## Changed files

Portal-enabled Storage bounds four sequential Growatt response windows strictly
below the five-second M2M deadline; native-only timing remains allowed. The
GraphQL runtime bridge and storage publisher both trigger Modbus RTU source-only
conformance fixtures.

Portal Storage reserves a fixed 500ms deterministic M2M headroom: four native
response budgets must be strictly below 4.5s. The real Portal-to-mTLS GraphQL
test uses four one-second delayed reads and succeeds without MCP priming.

The Portal-to-mTLS-GraphQL integration now uses four deterministic nonzero
native read delays and completes successfully without MCP priming, asserting
all four reads inside the admitted aggregate deadline.

The implementation adds the storage publication core and MCP tool, extends the
dedicated M2M GraphQL and Portal BFF paths, adds explicit configuration and
tests, and installs `scripts/growatt_storage_semreg_gate.sh` in local CI.
`scripts/transport_gate.sh` and `scripts/passive_smoke_gate.sh` preserve their
existing fail-closed checks while recognizing the explicit `PortalStorage`
public read-surface marker as non-transport/non-passive-capture configuration.

## Subsequent P2 corrections

- Correction code HEAD: `39a742da683869f46cca23228bc7904852d57446`
- Correction code tree: `dc40ef94810ea806dabf7e68d06ded3c24494b5c`
- Code commits: `87346de` (producer admission and canonical identity) and
  `39a742d` (strict gate classifier and hostile fixtures).
- Reviewed discussions: `discussion_r3974385693` and
  `discussion_r3974385706`; both received post-push author replies.

An enabled Portal Storage BFF now requires the Growatt RS-485 producer to be
enabled and its configured `AssetID` to exactly equal `PortalStorage.AssetRef`,
after dedicated M2M listener and allowlist validation. This does not enable
Growatt or alter independent Portal PV settings. A wholly zero Storage
configuration remains disabled.

Semantic identity validation now preserves the original value. It rejects
leading or trailing whitespace and values longer than 128 bytes before any
publication, public route, or endpoint construction. Both `AssetID` and
`SourceID` have leading-space, trailing-space, and overlength-before-trim
coverage; the 128-byte boundary remains accepted.

Focused evidence for this correction:

- `GOWORK=off go test -race ./ ./cmd/gateway ./m2mgraphql ./portal ./mcp -run
  'TestConfigCrossValidatesPortalStorage|TestGrowattSemanticIdentity|TestGrowattStorage|TestSemanticStorage|TestPortalBootstrap' -count=1` — PASS.
- `python3 scripts/transport_gate_test.py` — 23 PASS; `python3
  scripts/passive_smoke_gate_test.py` — 9 PASS.
- `GOWORK=off ./scripts/transport_gate.sh` — PASS (pinned Modbus RTU
  conformance); `./scripts/passive_smoke_gate.sh` — not triggered.

The first complete local CI run on `87346de` reached `golangci-lint` with zero
issues and then correctly stopped because the earlier strict config classifier
required a transport matrix for the new admission lines. `39a742d` extends only
the finite Storage-config allowlist and its hostile fixtures; its focused gate
tests pass. A fresh complete `GOWORK=off ./scripts/ci_local.sh` and a fresh
independent full-HEAD review remain pending for `39a742d`.

## Source epoch admission correction

SourceEpoch is now validated as the exact SemReg `SourceEpochID` before the
RTU endpoint is opened. Focused race tests reject whitespace, invalid/control
characters, and values over 256 bytes without opening an endpoint, while the
256-byte valid boundary opens. Portal Storage now rejects `RawReadEnabled=true`
instead of silently accepting an unused PV alias field. Full CI remains pending
by delivery-lead instruction.

The delivery-lead full-CI run for `db00a5f` passed Go race and Python suites,
then correctly stopped at the finite transport config classifier because the
three explicit Storage `RawReadEnabled` rejection lines were not listed. Its
log SHA-256 is `2efddab4275b01684a077a8507e0a0aaa069e36d6a351853c43b71e2bb2c3ba8`.
The classifier and hostile fixtures now list only those three lines; focused
transport classifier tests (23), passive classifier tests (9), and the actual
Modbus RTU production conformance gate pass. No full CI was run for this
report-only gate correction.

The Portal bootstrap now enables Storage only when the matching Growatt runtime
successfully started; an endpoint-open failure leaves its forwarder absent until
restart. The storage semantic publisher is classified as Modbus RTU transport
source, with focused race and direct conformance-gate evidence passing.

## Structural gate-classifier correction

Fresh review of `8cd99ff` found that a flattened line allowlist could accept a
Storage marker together with identical generic return or brace lines changed in
an unrelated validator. The first cardinality-only correction at `6ff8a41` was
rejected during lead integration because same-count line swaps remained possible.

The final correction uses one shared, fail-closed structural classifier from both
transport and passive gates. It removes exactly one canonical Portal Storage
declaration, `Config.PortalStorage` field, and the byte-exact reviewed
`ValidatePortalStorage` function from base and working source, then requires all
remaining `config.go` bytes to match. Missing, duplicate, reformatted, or changed
structures reject. Hostile fixtures cover unrelated `return err`, `return nil`,
and brace changes, same-count swaps inside another validator, deletion of matching
producer admission, and relaxation of the four-read timing bound. The exact
reviewed Storage slice remains exempt.

Focused evidence: `python3 scripts/transport_gate_test.py` — 23 PASS; `python3
scripts/passive_smoke_gate_test.py` — 9 PASS; direct transport gate — pinned
Modbus RTU conformance PASS; direct passive-smoke gate — not triggered; Python
compile and `git diff --check` — PASS. Complete CI and fresh exact-HEAD review
remain pending after the final commit.

## Context-aware serialization and exact identity correction

The production provider now acquires one capacity-one ownership gate with the
request context before entering the existing state mutex. A queued request whose
context expires returns without starting another RTU observation; a live queued
request proceeds after the current owner releases. Close remains serialized with
native and semantic publication. Deterministic race tests hold the first native
read, cancel a queued GraphQL/SemReg request, prove no second endpoint call, then
prove an admitted queued semantic request completes as the next four-read sample.
The queue tests pass 50 consecutive race-enabled runs.

Configured `AssetID` and `SourceID` are validated as their exact pinned SemReg
types before the endpoint opens. Leading/trailing whitespace, invalid first
characters, disallowed punctuation, and 257-byte values reject without calling
the endpoint; the 256-byte boundary for each type remains valid. The source epoch,
distinct-identity, native qualification, evidence, and publication checks remain.

Focused evidence after these changes: gateway/MCP/GraphQL/Portal race tests PASS;
queue tests 50/50 PASS under `-race`; transport classifier 23/23 PASS; passive
classifier 9/9 PASS; direct pinned Modbus RTU conformance PASS; passive smoke is
not triggered by the exact reviewed public Storage slice. Complete CI and a fresh
exact-HEAD independent review remain pending after commit and push.

## Direct GraphQL deadline and public contract correction

Direct mTLS GraphQL Storage remains reachable when Portal Storage is disabled.
Configuration therefore validates its four sequential response windows against
the server's ten-second write deadline independently of Portal. It reserves 500
ms and rejects an aggregate at or above 9.5 seconds; the exact just-below boundary
passes. The M2M runtime constructor invokes the same cross-configuration check,
while native-only/MCP composition with M2M disabled keeps its existing bounded
RTU timeout contract. Portal retains its stricter 4.5-second aggregate bound.

The public runtime-provider contract now names the required stable asset ID,
native and semantic MCP tools, mTLS GraphQL `SemanticStorageCurrent`, conditional
Portal endpoint, seven-fact SemReg projection, explicit losses, serialized
publication, and last-known-good behavior. Its migration section records the
Board's pre-v1 no-compatibility cutover rule instead of promising legacy semantic fallback,
shadow authority, adapters, or dual publication.

Focused configuration/runtime race tests, both structural gate suites, direct
Modbus RTU conformance, passive classification, and `git diff --check` pass.
Complete CI and fresh exact-HEAD independent review remain pending after push.

## Exact-HEAD independent-review blocker correction

This correction starts from independent review
`gateway959-92778b0-independent/REVIEW.md`, SHA-256
`8a675389a2a796af90e6f3c909402a2f95851f9272442062ce64433a8f47cd9a`,
against reviewed HEAD `92778b0ee27613f837cf0542478b8b3f34b66a1f` and tree
`39c1be10c95fdb35c1cc75b918cc19b6bfc35711`. It addresses only that report's
three reachable blockers; no deadline was extended and no live I/O, write,
fallback, comparator, compatibility surface, or second semantic authority was
introduced.

- `m2mgraphql/handler.go` is now a Modbus RTU production source input. The
  source-only trigger and fail-closed controls cover the handler dispatch,
  while an unrelated `m2mgraphql/handler_test.go` change remains non-triggering.
- Every transformed storage loss now carries the real native DefinitionID:
  `pack_current_amps`, `cumulative_charge_amp_hours`, or
  `cumulative_discharge_amp_hours`. Exact voltage, SOC, and temperature mappings
  still carry no loss, and operating-state continues to cite
  `native.growatt.bms.rs485.v202.operating_state`.
- Public Storage admission now proves the entire worst case,
  `MaxQuiescence + 4 * ResponseTimeout`, strictly below the 4.5-second Portal
  and 9.5-second direct-GraphQL budgets with 500 ms server headroom. The
  subtraction/division check is overflow-safe. Deterministic direct-GraphQL
  cancellation, retry through Portal, recovery, and last-known-good retention
  coverage exercises the fault path.

RED evidence was captured before the fixes: the new provenance and deadline
tests failed, and `python3 scripts/transport_gate_test.py` failed 2 controls
for `m2mgraphql/handler.go` being unclassified. GREEN focused race evidence:
`GOWORK=off go test -race ./ ./cmd/gateway ./m2mgraphql ./portal ./mcp -run
'Test(ConfigCrossValidatesPortalStorageAgainstGrowattProducer|GrowattStorageDispositionsUseExactNativeLossDefinitionIDs|GrowattStorageRecoveryCancellationRetryPreservesLastKnownGood|GrowattStorageGraphQLAndPortalPublishWithoutMCPPriming|GrowattBMSRS485)' -count=1`
passed. The hostile structural suites passed: transport 24/24 and passive 9/9.

Fresh complete validation passed before the correction commit:
`GOWORK=off ./scripts/ci_local.sh`. It recorded 93 Portal Node tests, all Go
race packages, 168 Python tests plus 6/24/9/6/2 gate suites, golangci-lint with
0 issues, pinned Modbus RTU conformance, Storage mapping (2 executable outputs
and 13 rejected scenarios), and passive-smoke not triggered. The complete log
is `wave12/gateway959-correction-final-full-ci.log`, SHA-256
`92c2a0a9b6bff261cf74d007a65bcfbcab26619fd620f70928c7f3985b5c474b`.

## Second exact-HEAD review correction

Independent review `gateway959-a67da0e-independent/REVIEW.md` (SHA-256
`696a7106e10879846e0d054ab6bbba0ec57c307926ed39ce2ac8cbc30365e536`)
identified two additional reachable paths. The M2M runtime now supplies every
verified request with a 9.5-second context deadline, preserving the 500 ms
write-response headroom. A native owner can hold the capacity-one gate while a
deadline-expired queued GraphQL request exits before a second RTU observation;
a later live request still succeeds. The runtime test observes the actual
provider deadline below the server write deadline.

The RTU source classifier now covers `cmd/gateway/portal_pv_client.go`,
`m2mgraphql/client.go`, and `portal/handler.go`; source-only pass/fail fixtures
cover each, while test-only changes remain non-triggering. No live I/O, write,
compatibility path, or semantic fallback was added.

RED: the provider context lacked a deadline and six new source-gate controls
failed. GREEN focused race tests and gate suites passed. Fresh complete
`GOWORK=off ./scripts/ci_local.sh` passed: 93 Portal Node tests, all Go race
packages, Python 168 plus 6/24/9/6/2 suites, lint 0 issues, RTU conformance,
mapping 2 outputs/13 rejects, and passive smoke not triggered. Log:
`wave12/gateway959-second-correction-full-ci.log`, SHA-256
`1a50d558bf5470d25356f13af66a046846bfad7b7a94d77afe5ffa9979638883`.
