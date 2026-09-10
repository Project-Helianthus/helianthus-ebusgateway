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
