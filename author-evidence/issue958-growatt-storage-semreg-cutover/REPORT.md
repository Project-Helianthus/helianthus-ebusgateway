# Issue #958 — Growatt Storage SemReg Cutover

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Branch: `issue/958-growatt-storage-semreg-cutover`
- Base commit: `32244901c4c8337266cd348bdad90fb9e8eb0a61`
- Base tree: `0a850d5646d46f5782b1396d72a3d93bffa6974e`
- Dependency pins: SemReg `f3f761bc67e10d6a65eba6c13cb4dc51002d6955`; docs-semantic mapping `f830ace6c2b9dd1af0e87ce808fa545662578418`.

The final local commit is the branch head returned with this report. Its exact
tree is recorded by Git in the commit object; no remote mutation was performed.

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

## Changed files

The implementation adds the storage publication core and MCP tool, extends the
dedicated M2M GraphQL and Portal BFF paths, adds explicit configuration and
tests, and installs `scripts/growatt_storage_semreg_gate.sh` in local CI.
`scripts/transport_gate.sh` and `scripts/passive_smoke_gate.sh` preserve their
existing fail-closed checks while recognizing the explicit `PortalStorage`
public read-surface marker as non-transport/non-passive-capture configuration.
