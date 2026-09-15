# Gateway #975 generation-bound B503 emission fence

Repository: `helianthus-ebusgateway`

Issue/branch: #552 / `issue/552-portal-ux`

Base: `588a89711830b313a565586454657d881c80229c`

## Scope

Fix valid finding `r4012260911`: `AdmitPendingEmission` previously marked a
typed B503 lifecycle operation as emitted, released `stateMu`, and only then
called `bus.Send`. `OnEpochAdvance` could win that interval and make the old
operation write via the replacement bus generation.

`Manager.BeginPendingEmission` now holds a dedicated emission fence from typed
admission through the return from the generation-bound `bus.Send` call. The
dispatcher uses that fenced admission for every pending lifecycle operation.
`OnEpochAdvance` takes the same fence before it records a new generation. If
the epoch wins first, admission fails without a write; if admission wins, the
send enters only the issuing generation and the epoch advances afterwards.

The owner gate and B524 `readMu` keep their existing order. Transport
disconnect remains independent of this fence so it can still cancel an already
entered, blocked `Send` without waiting for that send to return.

## Deterministic control

`TestIssue552B503EpochAdvanceAfterAdmissionDoesNotWriteReplacementGeneration`
uses a dispatcher seam exactly after typed admission. It starts an epoch advance
while the emission fence is held, then verifies the only SERVICE_WRITE `00 03`
enters `bus.Send` with dispatch epoch 1, before the advance can complete. Once
the send returns, the advance completes to epoch 2. The test runs 50 times in
the focused repeat control and under the race-concurrency suite.

## Validation

All Go commands used `CGO_ENABLED=0 GOWORK=off`; the local macOS SDK linker is
incompatible with the installed Go toolchain when CGO is enabled.

| Command | Result |
| --- | --- |
| focused dispatcher pre/post-emission regressions | PASS |
| epoch-send fence control and post-emission disconnect control, `-count=50` | PASS |
| `go test ./internal/vaillant/b503session -count=1` | PASS |
| `go test -race -tags=raceconcurrency ./cmd/gateway -run 'TestM6Conc|TestIssue552B503EpochAdvanceAfterAdmissionDoesNotWriteReplacementGeneration' -count=1` | PASS |
| `go test ./cmd/gateway -count=1` | PASS |
| `go test ./mcp -run B503 -count=1` | PASS |
| `go test ./graphql -run B503 -count=1` | PASS |
| `go vet ./cmd/gateway ./internal/vaillant/b503session` | PASS |
| `git diff --check` | PASS before staging |

No Portal or adaptermux files, review action, feedback resolution, merge, or
live action was performed.
