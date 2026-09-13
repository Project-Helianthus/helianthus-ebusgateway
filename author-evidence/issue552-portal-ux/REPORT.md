# Issue 552 Portal UX author evidence

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: #552
- Branch: `issue/552-portal-ux`
- Base and initial worktree HEAD: `34c8a5d8a5444a7f5a8d6350c7b1258af665bb0a`
- Base tree: `8e101f29ccbc86910185237eb1a99ba7e7a0ef84`
- Candidate source HEAD: `5c1c0e685e928be0aa3afedf5140b393413e9102`
- Candidate source tree: `d23c773ac11e4b4ae43952d33d18e8317073e0fb`

## Scope completed

The B503 Portal path is GraphQL-only. It carries `targetAddress` through
capability, current errors, service, history, live monitor, and session-state
queries; target changes advance a frontend epoch, discard stale results, and
disable an abandoned enable completion on its original target. The UI has all
five documented state selectors, a bounded history tab, gateway-owned session
strip, nav-away cleanup, the AD02 banner and canonical tooltip anchor, and a
capability-gated projection card.

The GraphQL surface adds bounded `vaillantErrorsHistory` and
`vaillantLiveMonitorSession`. The MCP capability probe now has a target-bound
form; it preserves the existing bounded probe and native dispatcher ownership.
No REST path, browser MCP/native fallback, compatibility semantic owner,
dual publication, install action, live device operation, credential, deploy,
hardware, or 0.8 work was added.

## Validation

- `node --test portal/web/test/*.test.mjs`: PASS, 104 tests.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./mcp ./graphql ./cmd/gateway -run 'TestVaillantB503' -count=1`: PASS.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./graphql -count=1`: PASS.
- `./scripts/build_portal_assets.sh` regenerated the tracked embedded asset and manifest; `./scripts/check_portal_assets.sh`: PASS after staging generated assets.
- First full `ci_local.sh` run reached repository-wide race testing and failed only because the schema-characterization test correctly detected the two new root fields. The expected root field set and schema digest were then updated; its log is retained as `ci_local-first.log`.
- Final `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off ./scripts/ci_local.sh`: PASS. It includes 104 Portal Node tests, repository-wide race tests, source-selection coverage, 219 Python tests, `golangci-lint`, declared mapping gates, and the non-triggered transport/passive-smoke gates. Its durable log is `ci_local-final.log`.

## Gate boundary

`Project-Helianthus/helianthus-docs-ebus#523` remains open. This code is not
claimed merge-ready, has not been pushed, and no PR, issue, Project, or remote
state was mutated.
