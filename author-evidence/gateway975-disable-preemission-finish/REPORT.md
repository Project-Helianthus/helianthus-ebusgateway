# PR 975 Disable pre-emission author evidence

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: #552
- Branch: `issue/552-portal-ux`
- Starting pushed HEAD: `c4f5f4c5e7aa61a5ae2c3500d27be46792f2167d`
- Feedback corrected: `r4012109347`

## Completed correction

`DisableOperation` now distinguishes an outcome that reached `bus.Send` from a
context or poll-quiesce failure that occurred before emission. When the current
owner remains `Active`, a non-emitted Disable removes only its pending record,
re-arms the idle timer, and returns the exact failure. It preserves the issuer
token and the pre-existing emitted-Enable cleanup evidence, without marking a
new cleanup attempt or inventing an emitted native result. The caller can retry
the same authenticated Disable.

The established disconnect path remains unchanged: if an authoritative
transport withdrawal has already transitioned the manager away from its active
owner, the conservative cleanup fence records that transport outcome. This
correction does not change restart settlement behavior or `r4012109336`.

## Validation

- `GOWORK=off CGO_ENABLED=0 go test -race -count=5
  ./internal/vaillant/b503session`: PASS. The new core regression proves a
  non-emitted Disable keeps the owner, adds no cleanup attempt, and permits a
  later successful retry.
- `GOWORK=off CGO_ENABLED=0 go test -race -count=1 ./cmd/gateway -run 'B503'`:
  PASS. The new production dispatcher regression holds `readMu` until a caller
  deadline expires, proves `bus.Send` count is zero, verifies existing cleanup
  evidence remains unattempted and the issuer remains active, then retries.
- `GOWORK=off CGO_ENABLED=0 go test -race -tags=raceconcurrency -count=1
  ./cmd/gateway -run 'B503'`: PASS.
- `GOWORK=off CGO_ENABLED=0 go test -race -count=1 ./graphql ./mcp -run 'B503'`:
  PASS.
- `GOWORK=off CGO_ENABLED=0 go vet ./internal/vaillant/b503session
  ./cmd/gateway ./graphql ./mcp`: PASS.
- `git diff --check`: PASS.

No live device, deployment, credential, installation, compatibility fallback,
review, feedback-resolution, or merge action was performed.
