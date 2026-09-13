# Issue 552 Portal UX author evidence

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: #552
- Branch: `issue/552-portal-ux`
- Base and initial worktree HEAD: `34c8a5d8a5444a7f5a8d6350c7b1258af665bb0a`
- Base tree: `8e101f29ccbc86910185237eb1a99ba7e7a0ef84`
- Initial candidate source HEAD: `5c1c0e685e928be0aa3afedf5140b393413e9102`
- Initial evidence HEAD reviewed with blockers: `d283052ed1969582346505654d080c1109cf85e1`
- Corrected source HEAD: `4a8bb9fdf2e4a451fc815583dcba2ef222dc01bf`
- Corrected source tree: `3c5b400f6bfddc4edf5ea39b609efaf9289f5c11`

## Scope completed

The B503 Portal path is GraphQL-only. It carries `targetAddress` through
capability, current errors, service, history, live monitor, and session-state
queries; target changes advance a frontend epoch, discard stale results, and
disable an abandoned enable completion on its original target. The UI has all
five documented state selectors, a bounded history tab, gateway-owned session
strip, nav-away cleanup, the AD02 banner and canonical tooltip anchor, and a
capability-gated projection card.

The GraphQL surface adds bounded `vaillantErrorsHistory` and
`vaillantLiveMonitorSession`. Both new fields now delegate to the same typed
operations as stable, core-classified `ebus.v1.vaillant.errors.history.list`
and `ebus.v1.vaillant.live_monitor.session.get` tools. Their schemas and
deterministic envelopes have golden snapshots, and executable integration tests
cover MCP-to-GraphQL value, ordering, null-slot, and all-or-nothing error parity.
The MCP capability probe now has a target-bound form; it preserves the existing
bounded probe and native dispatcher ownership.
No REST path, browser MCP/native fallback, compatibility semantic owner,
dual publication, install action, live device operation, credential, deploy,
hardware, or 0.8 work was added.

The exact-head review of `d283052ed...` reported six P1/P2 findings and one P3.
The correction graduates the two new eBUS contracts through stable MCP, keeps
the target selector in every availability state, preserves null as the configured
default target, renders the B503 card without projection planes, describes
`SESSION_BUSY` as ownership-or-lifecycle contention, and gates rejected stale
requests by the current target epoch. The session strip reports only that the
gateway gate is held; it does not infer a foreign client from a missing browser
token.

## Validation

- `node --test portal/web/test/*.test.mjs`: PASS, 107 tests.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./mcp ./graphql ./cmd/gateway -run 'TestVaillantB503|TestIssue552VaillantB503|TestToolClassificationPolicy' -count=1`: PASS.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./graphql -count=1`: PASS.
- `./scripts/build_portal_assets.sh` regenerated the tracked embedded asset and manifest; `./scripts/check_portal_assets.sh`: PASS after staging generated assets.
- First full `ci_local.sh` run reached repository-wide race testing and failed only because the schema-characterization test correctly detected the two new root fields. The expected root field set and schema digest were then updated; its log is retained as `ci_local-first.log`.
- Final `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off ./scripts/ci_local.sh`: PASS. It includes 104 Portal Node tests, repository-wide race tests, source-selection coverage, 219 Python tests, `golangci-lint`, declared mapping gates, and the non-triggered transport/passive-smoke gates. Its durable log is `ci_local-final.log`.
- Corrected-head full CI ran the same command on `4a8bb9f...`: PASS. It includes
  107 Portal tests; native and Linux builds; repository-wide race tests; Python
  168+6+26+11+6+2; zero lint findings; Storage and EVSE SemReg mapping gates;
  and correctly non-triggered transport/passive-smoke gates. Durable log:
  `ci_local-correction-4a8bb9f.log`, SHA-256
  `6fb567bddf4c83a07aedd9475ac4b0a40a4a1f8258273c966950c08d2d0a5781`.

## Gate boundary

`Project-Helianthus/helianthus-docs-ebus#523` / PR #524 and this repository's
PR #975 remain open. Docs #524 received two new P2 findings after its first
corrected-head review and is being corrected. This source commit is not claimed
merge-ready and has not been pushed; fresh exact-HEAD review remains required
after the final evidence commit and accepted docs gate.
