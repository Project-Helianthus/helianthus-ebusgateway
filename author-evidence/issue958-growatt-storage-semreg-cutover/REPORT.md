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

## Structural gate-classifier correction

Fresh review of `8cd99ff` found that a flattened line allowlist could accept a
Storage marker together with identical generic return or brace lines changed in
an unrelated validator. The first cardinality-only correction at `6ff8a41` was
rejected during lead integration because same-count line swaps remained possible.

The final correction uses one shared, fail-closed structural classifier from both
transport and passive gates. It removes exactly one canonical Portal Storage
declaration, `Config.PortalStorage` field, and balanced
`ValidatePortalStorage` function from base and working source, then requires all
remaining `config.go` bytes to match. The balanced scanner handles nested blocks,
comments, quoted strings, runes, and raw strings. Missing or duplicate structures
reject. Hostile fixtures cover unrelated `return err`, `return nil`, and brace
changes, including same-count swaps inside another validator; the valid
Storage-only change remains exempt.

Focused evidence: `python3 scripts/transport_gate_test.py` — 23 PASS; `python3
scripts/passive_smoke_gate_test.py` — 9 PASS; direct transport gate — pinned
Modbus RTU conformance PASS; direct passive-smoke gate — not triggered; Python
compile and `git diff --check` — PASS. Complete CI and fresh exact-HEAD review
remain pending after the final commit.
