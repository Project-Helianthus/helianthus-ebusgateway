# PR 975 startup and idle-read author evidence

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: #552
- Branch: `issue/552-portal-ux`
- Starting pushed HEAD: `9fda81b577c7eaf523528fd4853209549135aa71`
- Feedback corrected: `r4011985541` and `r4011985545`

## Completed correction

Production B503 installation now invokes `ResetForRestart(defaultVaillantTarget)`
before registering MCP or GraphQL consumers. The configured target is therefore
restart-fenced before public availability or Enable exposure. An explicitly
requested target is recorded as qualified by the existing target-bound
availability path and inherits that same restart fence. Cold-start availability
is `UNKNOWN`; no successful dispatch is fabricated and Enable makes no native
emission while the conservative fence remains.

The idle callback now recognizes an admitted `OperationRead`. If its timer
expires while the read is waiting for its native outcome, it records an elapsed
expiry but does not release the owner or dispatch cleanup under the read. A
successful ACK clears that deferred expiry and re-arms the timer. A non-ACK or
non-transport failure consumes the already elapsed expiry into one target- and
epoch-bound defensive cleanup after the read returns. Transport-down retains
its established immediate teardown path.

## Validation

- `GOWORK=off CGO_ENABLED=0 go test -race -count=5
  ./internal/vaillant/b503session`: PASS. The repeated session suite includes
  deterministic blocked-read races: cleanup cannot cross the pending read; ACK
  re-arms; and a failed read after expiry runs exactly one cleanup.
- `GOWORK=off CGO_ENABLED=0 go test -race -count=1 ./cmd/gateway -run 'B503'`:
  PASS. The production-install regression asserts `UNKNOWN` for configured and
  newly qualified targets and zero cold-start Enable dispatches.
- `GOWORK=off CGO_ENABLED=0 go test -race -count=1 ./graphql ./mcp -run 'B503'`:
  PASS.
- `GOWORK=off CGO_ENABLED=0 go vet ./internal/vaillant/b503session
  ./cmd/gateway ./graphql ./mcp`: PASS.
- `git diff --check`: PASS.

No live device, deployment, credential, installation, transport-conformance,
review, feedback-resolution, or merge action was performed.
