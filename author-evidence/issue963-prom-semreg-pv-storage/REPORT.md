# Issue 963 author evidence

## Final correction evidence

Exact validated head: `f1f90fe7cfc17b6db847cf404c8fabb2af5f973d`.
Tree: `aec5e23e379b23fb7259adede1d434f3452443e7`.

`dcb59ab` failed the Modbus adapter race suite because its normal PV accessor
incorrectly bypassed the injected wall/monotonic clocks. Commit `3180779`
restored that established path; this failure is retained here as repaired
evidence, not a passing result. The final focused race command
`GOWORK=off go test -race -count=1 . ./cmd/gateway ./internal/modbusadapter`
passed: root 9.666s, gateway 95.677s, Modbus adapter 132.724s.

On the same head `GOWORK=off ./scripts/ci_local.sh` passed: Portal 93/93,
complete Go race suite, Python suites 168/6/24/10/6/2 passing, golangci-lint
0 issues, transport conformance PASS, Storage SemReg gate PASS (2 executable,
13 rejected), and passive smoke classifier `not triggered`.

Post-correction exact head `c19cb601e69e69efda0a4f6698b219ee331678cd`
also passed the focused race: root 9.700s, gateway 96.065s, adapter 130.581s;
and full `ci_local.sh`: Python 168/6/24/10/6/2, lint 0, transport PASS,
Storage 2/13 PASS, passive classifier not triggered. Hosted CI run
`34467421336` was 4/4 SUCCESS on that same commit.

Branch: `issue/963-prom-semreg-pv-storage` from gateway main
`b2651d73639efb7ba690bc1464d9b8b04df51e4a`.

The change appends optional PV and Storage SemReg observations to the existing
`/metrics` renderer. It snapshots each detached domain outside the observability
store lock; PV uses its existing no-I/O current accessor and Storage reads only
the publication's locked `Current` bytes. The latter path does not call
`SemanticStorageCurrent`, `observeLocked`, native Modbus, MCP, GraphQL, or
Portal.

The renderer fails closed for a non-coherent tuple, unapproved pack/fact/item,
dimension, unit, invalid key, duplicate sample, conflict, non-qualified,
non-promoted, invalid, unavailable, or non-fresh fact. Labels use fixed domain,
accepted mapping vocabulary and finite dimensions; no asset/source/evidence or
free-text label is emitted. Existing eBUS output is unchanged when no semantic
provider is wired.

Validation completed:

- `GOWORK=off go test . -run TestSemanticPrometheus -count=1` — PASS.
- `./scripts/growatt_storage_semreg_gate.sh` — PASS: 2 executable outputs and
  13 rejected scenarios; pinned Storage pack test PASS.
- `GOWORK=off go test -race -count=1 . ./cmd/gateway
  ./internal/modbusadapter` — PASS (root, gateway, and Modbus adapter).
- `GOWORK=off ./scripts/ci_local.sh` — PASS: portal 93/93, Go race suite,
  Python suites including passive gate 10/10, lint 0 issues, transport gate,
  Storage mapping gate, and the semantic-only passive classifier. The gate
  remains fail-closed for passive state, locking, or counter changes and does
  not use a report or override for this detached renderer append.

Documentation gate: required and satisfied by
`docs/semantic-prometheus-pv-storage-v1.md`. Transport gate: applicable local
CI transport gate passed; this change introduces no transport behavior. Smoke:
offline semantic composition only; no live device, credential, deployment, or
hardware action was performed.

## Current author validation

Current implementation head: `d0c1ed764371efc4287d6a381eb63a54b46abe99`.
Current implementation tree: `f7180dbabf50ed386bc70dc5308d7ba95af574d7`.

This correction closes the unbounded PV projection-item label path by admitting
only the fourteen PV IDs declared by the accepted gateway projection catalog.
Unknown suffixes are omitted and counted as render overflow. The renderer also
rejects invalid canonical fact keys explicitly, counts invalid state vocabulary,
and tests configured-startup outage versus disabled-domain registration. The
passive-smoke classifier regression fixture now rejects a missing semantic
unlock even if an unrelated unlock is added elsewhere.

- `GOWORK=off go test -race -count=1 . ./cmd/gateway ./internal/modbusadapter`
  — PASS: root 9.502s, gateway 95.532s, Modbus adapter 130.696s; durable log
  `/tmp/helianthus-ebusgateway-963-focused-race.log`, SHA-256
  `bb67b6c7658c5b7e20168d31fd8f5ebcc84ba49f77768b272d5706c8cd2f7c22`.
- `GOWORK=off ./scripts/ci_local.sh` — PASS: portal 93/93; full Go race
  suite; Python suites 168/6/24/10/6/2; golangci-lint 0 issues; transport
  gate PASS; Storage SemReg mapping gate PASS (2 executable outputs, 13
  rejected scenarios); and passive smoke gate `not triggered`. Durable log
  `/tmp/helianthus-ebusgateway-963-full-ci.log`, SHA-256
  `3c05187da1c2c91ebedd0d6c43e714b5318a73731135cb88cf4469417563b1a0`.

Hosted run status was 4/4 SUCCESS on the implementation head before this
author-evidence-only update. No physical smoke applies: this issue is offline,
read-only metric composition with no device, credential, transport acquisition,
publication, Portal, GraphQL, or MCP operation.

## Dimension and identity-rotation correction

Current implementation head: `6f84e8cdb56d6c870ac7e71d750498c86354091a`.
Current implementation tree: `15bb1659a33fc8c0f8f0acb42b3d9156a05eb022`.

Every rendered PV or Storage fact now requires one accepted dimension. A missing
dimension is malformed, emits no fact state/value/age/conflict sample, and is
counted as render overflow; hostile coverage exercises both domains. The PV
publication core retains historical identity-keyed public evidence but records
one latest accepted active asset for the single-domain scrape view. The
deterministic rotation test proves a new SunSpec Common identity remains
available to metrics while the prior evidence remains addressable by asset, and
the read-only scrape path performs no native acquisition or publication.

- `GOWORK=off go test -race -count=1 . ./cmd/gateway ./internal/modbusadapter`
  — PASS: root 9.437s, gateway 94.851s, Modbus adapter 130.346s; durable log
  `/tmp/helianthus-ebusgateway-963-focused-race.log`, SHA-256
  `52d5fe2671c1852a4cbd57f5ce43f39b7a0c7bbfd60b47b8caeae96864e14055`.
- `GOWORK=off ./scripts/ci_local.sh` — PASS: portal 93/93; full Go race
  suite; Python suites 168/6/24/10/6/2; golangci-lint 0 issues; transport
  gate PASS; Storage SemReg mapping gate PASS (2 executable outputs, 13
  rejected scenarios); and passive smoke gate `not triggered`. Durable log
  `/tmp/helianthus-ebusgateway-963-full-ci.log`, SHA-256
  `d769e96a2ccddaf29f292db3a8e642d67e60c6043bb3b280e61c30c2a0c02880`.

## Storage wall-rollback correction

Current implementation head: `b29e4b0e8203381f7b5680fb39436bba3dc3523c`.
Current implementation tree: `45d7433bcbf0d1b8973f2d01d5c57588ad4056d6`.

Storage scrape reevaluation still derives freshness solely from receipt-to-scrape
monotonic elapsed time. It now clamps the serialized evaluation wall coordinate
to the detached receipt wall floor, preventing a backward wall adjustment from
making `EvaluateSnapshot` reject an otherwise valid tuple. Deterministic
rollback and forward-step cases cover fresh/stale boundaries, no resurrection,
no early expiry, receipt-wall clamping, and no native read/publication.

- `GOWORK=off go test -race -count=1 . ./cmd/gateway ./internal/modbusadapter`
  — PASS: root 9.595s, gateway 95.586s, Modbus adapter 130.811s; durable log
  `/tmp/helianthus-ebusgateway-963-focused-race.log`, SHA-256
  `0ce5d1778fa51b7f3d1230da356988fe835974a45bfa8611520573f5bfe0a548`.
- `GOWORK=off ./scripts/ci_local.sh` — PASS: portal 93/93; full Go race
  suite; Python suites 168/6/24/10/6/2; golangci-lint 0 issues; transport
  gate PASS; Storage SemReg mapping gate PASS (2 executable outputs, 13
  rejected scenarios); and passive smoke gate `not triggered`. Durable log
  `/tmp/helianthus-ebusgateway-963-full-ci.log`, SHA-256
  `5a4496e7ed8b9e801cb60f749a53fdb407d8cb8330ef6a4c31ee19c8072c92d5`.
