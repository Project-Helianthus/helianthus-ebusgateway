# Issue #469 author checkpoint: daemon update status

## Scope and base

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/469
- Exact base: `008023cdf1c290067c5bd9f058dc10785608c3f5`
- Branch: `issue/469-daemon-update-status`

## Delivered daemon contract

GraphQL daemon status (including the existing camelCase and snake_case aliases)
and `ebus.v1.runtime.status.get` now expose the already validated embedded
`gatewayBuildInfo.ReleaseVersion` as `firmwareVersion`.

`internal/releasecheck` is a bounded, injectable background cache. It requests
the newest successful public `push` workflow run on add-on `main` for
`build.yml`, validates that run, and reads `helianthus/config.json` through the
GitHub contents API at that exact immutable `head_sha`. It compares versions
with `golang.org/x/mod/semver`. It does not consult a moving `main` file or a
GitHub Release tag.

The HTTP client, clock, refresh interval, response size, and request timeout
are injectable. Refresh is single-flight and rate-limited. Before a successful
comparison the status fails closed to `false`; a failed later refresh retains
the previous successful result. GraphQL and MCP status calls only read that
cache and never issue network requests.

The adapter result remains `updatesAvailable=false`. There is no public
adapter catalogue proving a latest compatible firmware for its model, hardware
revision, and bootloader, so comparing it to the add-on version would be
incorrect.

## Files

- `internal/releasecheck/checker.go`: public-build discovery, exact-SHA config
  retrieval, semantic comparison, cache, and bounded background refresh.
- `internal/releasecheck/checker_test.go`: fake HTTP/clock semantic, exact-SHA,
  failure-retention, request-free status, and concurrency coverage.
- `cmd/gateway/status_provider.go` and composition: shared cached release state
  in GraphQL and MCP providers.
- `cmd/gateway/status_provider_test.go` and
  `cmd/gateway/testdata/issue469_runtime_status.golden.json`: server-level MCP
  serialization coverage for the non-empty daemon release and cached update
  result, including the deterministic `data_hash`.
- `cmd/gateway/status_provider_test.go` and
  `cmd/gateway/testdata/issue469_graphql_daemon_status.golden.json`: executable
  GraphQL queries for both daemon-status aliases and field-level parity against
  the MCP runtime-status serialization from the same fixture.
- `docs/daemon-update-status.md`: source, cache, failure, and adapter boundary.

## Validation

Focused normal and race tests:

```text
GOWORK=off go test -race ./internal/releasecheck ./cmd/gateway \
  -run 'TestChecker|TestIssue469(DaemonStatusUsesEmbeddedReleaseAndSharedCachedComparison|MCPRuntimeStatusSerializesCachedDaemonReleaseGolden|GraphQLDaemonStatusAliasesMatchMCPRuntimeStatus)' -count=1
PASS
```

The server-level MCP golden was introduced after the PR review correctly noted
that direct provider tests cannot protect the stable serialized envelope. Its
RED-first run failed because the intentional golden did not yet exist; the
GREEN run pins `firmware_version: "0.6.56"`, `updates_available: true`,
`initiator_address: "auto"`, and data hash
`c7c819c9732585417ab480a19b08bb4628c1c156f1a178289ea10780c3decb22`.

The independent exact-HEAD review then identified the remaining GraphQL parity
gap. The RED-first executable alias test failed while its expected public
response was absent; the GREEN test fixes both `daemonStatus` and
`daemon_status` at `firmwareVersion`/`firmware_version` `"0.6.56"` and
`updatesAvailable`/`updates_available` `true`, and compares both field pairs to
the MCP runtime-status payload from the same cached-release fixture. The MCP
golden and its data hash are unchanged.

Full repository gate, finalized implementation tree:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

The complete local CI passed terminology and source-selection gates, gofmt,
portal Node tests (93 passed), asset verification, vet, native and Linux 32-bit
builds, full Go race tests, canonical PV SemReg shadow, schema coverage, Python
script tests, golangci-lint (0 issues), and the non-applicable transport and
passive-smoke gates.

## Gates and residual risk

- Documentation gate: satisfied by `docs/daemon-update-status.md`.
- Transport gate: not triggered; no transport or protocol behavior changed.
- Runtime/smoke gate: not triggered; this is public read-only release metadata
  and no device or add-on action was performed.
- Hosted checks and fresh independent review: pending the linked PR.

The remaining risk is the availability or contract stability of GitHub's public
API. The checker is bounded, fails closed before its first valid result, and
preserves a previously valid comparison during a refresh failure.

## Author return boundary

No credentials, downloads, installations, deployments, add-on edits, or device
actions were performed. The adapter catalogue criterion remains open. This
checkpoint stops after commit, push, linked PR creation, and local evidence;
review, feedback resolution, merge, and issue closure remain with the Delivery
Lead.
