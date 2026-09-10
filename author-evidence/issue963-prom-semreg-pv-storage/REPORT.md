# Issue 963 author evidence

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
- Earlier focused `GOWORK=off go test -race . ./cmd/gateway
  ./internal/modbusadapter` passed before the final renderer hardening; the
  subsequent full focused rerun was interrupted by the pre-existing long suite
  after the renderer test failure that was fixed locally.
- `GOWORK=off ./scripts/ci_local.sh` completed all build, race, lint, transport,
  and Storage mapping stages, then stopped at the repository passive-smoke gate:
  changing `bus_observability_store.go` structurally requires
  `PASSIVE_SMOKE_REPORT`. No report was supplied or fabricated because this
  issue has an explicitly offline/no-live smoke boundary.

Documentation gate: required and satisfied by
`docs/semantic-prometheus-pv-storage-v1.md`. Transport gate: applicable local
CI transport gate passed; this change introduces no transport behavior. Smoke:
offline semantic composition only; no live device, credential, deployment, or
hardware action was performed.
