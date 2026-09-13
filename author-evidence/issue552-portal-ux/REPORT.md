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
- Final selector-aligned source HEAD: `d8364b4e9c727905b730ef4cf077bcfd8b544e3d`
- Final selector-aligned source tree: `ae2ebd5a1aa006b67e5a3bfe8f26fb9fb2ede27b`
- Final feedback-corrected source HEAD: `ffa83350b80dd4f763ff7138e8664dcb6390a8de`
- Final feedback-corrected source tree: `661e4ce09f43b2013408246936c4757637d0c036`

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
- After the final docs contract retained `data-role="projection-b503-card"`,
  `d8364b4...` added that public selector alongside the test selector and an
  executable assertion. The complete CI command was rerun on that exact source
  and passed with the same gate set. Durable log:
  `ci_local-final-d8364b4.log`, SHA-256
  `154962da1f28e76204551a160f386e58ca7eba88f8deaf9a8a5be757f5119aa5`.
- Live feedback then identified two reachable gaps: a nonempty discovered-device
  list could auto-select a different target after probing the configured default,
  and an absent GraphQL provider fabricated an Idle session. `ffa8335...` keeps
  an explicit selected configured-default option until a target change triggers
  a new probe, and returns `NOT_SUPPORTED` when the session provider is absent.
  Focused browser and GraphQL tests cover both. The complete CI command was rerun
  on this source and passed with Portal 108 and the same full gate set. Durable
  log: `ci_local-final-ffa8335.log`, SHA-256
  `5a1d8c5368b2e0e381a44addaae7086cbeb84ff087af6b5adf43c8cfb07cfc84`.
- Review feedback then identified two further P2 gaps. `27d323f...` makes the
  MCP-owned session status use one `stateMu` snapshot for its normalized state
  and ownership gate, so the GraphQL adapter receives the same coherent view.
  Its deterministic manager test observes the blocked epoch-refresh state and
  confirms that the internal `expired` state is normalized while ownership
  remains held. The Portal now treats the state written by a projection card as
  canonical; a stale hidden B503 target picker cannot replace it. The browser
  regression clicks a card for target 21 while the stale picker contains 8 and
  verifies both capability and later error requests use 21. The no-provider
  GraphQL `NOT_SUPPORTED` behavior remains covered by its existing test.
- Focused validation on `27d323f...`: `node --test
  portal/web/test/vaillant-b503.test.mjs` passed 14 tests; `go test -race`
  across B503 session, MCP, GraphQL, and gateway focused cases passed.
- Final `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on the source tree of `27d323f...`: PASS.
  It includes 109 Portal Node tests, repository-wide race tests, source schema
  validation, Python 168+6+26+11+6+2, zero lint findings, Storage and EVSE
  SemReg mapping gates, and correctly non-triggered transport/passive-smoke
  gates. Durable log: `ci_local-final-27d323f.log`, SHA-256
  `e0bb37189fce8078bf6e7c4db13b2f9f4b254b5e22c2257a3593f8f2e3f7f7cd`.
- A final full inventory found two more P2 gaps. `3dc72ce...` keeps the
  established default-target envelope behavior for unrelated B503 tools, but
  binds `errors.history.list` and `live_monitor.session.get` metadata to their
  already-resolved `target_address` through `VaillantB503AvailabilityAtCtx`.
  The multi-device regression makes only target 21 available while the default
  target 8 fails, then proves both promoted tools report target 21 as
  `AVAILABLE`. The canonical runtime-provider inventory now names all seven
  stable B503 operations, including the core-stable history aggregate and
  gateway-held session read; it keeps `live_monitor.get` distinct as the
  bounded session action surface. No docs-ebus artifact was changed.
- Focused `go test -race` across MCP, GraphQL, and gateway B503 cases passed.
  Final `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `3dc72ce...`: PASS, including 109
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-3dc72ce.log`, SHA-256
  `9877795bd87c898a968d6daf65e8dbcabade87ca1cffa9550b4dec35485907df`.
- Hosted-review feedback then found two final field-isolation gaps.
  `91eb2bc...` makes the `vaillantErrorsHistory` GraphQL root nullable while
  retaining non-null list elements. An indexed aggregate failure still returns
  no partial history and `UPSTREAM_RPC_FAILED`, but now nulls only that root
  field and preserves siblings. The mixed-root GraphQL and production
  MCP-to-GraphQL parity regressions prove that behavior; the schema
  characterization digest and public runtime contract record the deliberate
  nullability change. Portal projection loading now assigns every invocation a
  generation. The deterministic A->B->A test delays all three capability
  replies, resolves the first two after they are stale, and proves only the
  latest A invocation appends a card.
- Focused `go test -race` across GraphQL, MCP, and gateway B503 cases passed;
  focused Portal B503 tests passed 15/15. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `91eb2bc...`: PASS, including 110
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-91eb2bc.log`, SHA-256
  `4275137a7f26f35310cf32b5479dbfc75118b5b36ebe7f0e075ec58b2527d582`.
- The final hosted-review P2 found that an absent provider could still null an
  entire mixed GraphQL query through the non-null
  `vaillantLiveMonitorSession` root. `9442071...` makes that root nullable,
  while `state` and `owned` remain non-null whenever a session object exists.
  The mixed-root regression proves the field is null with a structured
  `NOT_SUPPORTED` error and an intact `vaillantCapabilities` sibling reporting
  `NOT_SUPPORTED`; the schema characterization digest and public runtime
  contract record the intentional root-nullability change.
- Focused GraphQL/MCP/gateway `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `9442071...`: PASS, including 110
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-9442071.log`, SHA-256
  `aba3764450865181c5e51f24ca119aef461dc2a19585865c50e53a71b915ca66`.
- The final exact-head Portal P2 found that nav-away cleanup cleared the local
  issuer token after an HTTP-successful GraphQL error. `6aa2f01...` now clears
  the token and remembered target only when GraphQL returns `disabled: true`.
  A GraphQL error, absent confirmation, or transport failure retains them and
  reports `Disable pending` without automatic retry; tab swaps and target
  changes use the same bounded cleanup behavior. The browser regression uses
  an HTTP-successful `SESSION_BUSY` GraphQL error and proves token/target
  retention, truthful pending status, and exactly one request. The existing
  success regression continues to prove confirmed disable clears the token.
- Focused Portal B503 tests passed 16/16 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `6aa2f01...`: PASS, including 111
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-6aa2f01.log`, SHA-256
  `fa0f3dd7b2b079922b69b81a28c41a74ea3f9413e8d5118bfcda8c378201cfbd`.
- Independent exact-HEAD review identified one remaining late-enable P2. A
  target-A enable response arriving after selection moved to B supplied an
  issuer token that was used only for one direct cleanup attempt; an
  unconfirmed result discarded the only browser recovery handle. `0426b0d...`
  stores the late A token with its captured A target before routing cleanup
  through the confirmation-aware helper. Failed GraphQL/transport/absent
  confirmation retains that pair for a later user-triggered bounded cleanup;
  `disabled: true` clears it without changing B's selection or epoch. The
  deterministic A->B regression injects a late A enable, fails its first A
  disable, verifies A token/target retention and B isolation, then confirms a
  single later cleanup clears exactly that A pair.
- Focused Portal B503 tests passed 17/17 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `0426b0d...`: PASS, including 112
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-0426b0d.log`, SHA-256
  `def443fdd01f9bbc51526cb2f7537db5306b4bad6aa2b4ce31fd9a96c8bc2192`.
- Late exact-head review then identified a concurrent cleanup ownership gap.
  `7a5879d...` snapshots the old token/target pair before awaiting its disable
  result and clears local recovery state only when that captured pair is still
  current. A delayed confirmed cleanup for A therefore cannot erase a B token
  and target installed by a reopened pane/new enable. The deterministic browser
  regression starts A cleanup, installs B while A is pending, confirms A,
  verifies B and its active status survive, then confirms the later explicit B
  cleanup uses `token-B` on target 21 and clears only B. Previous late-enable
  and failure-retention behavior remains covered.
- Focused Portal B503 tests passed 18/18 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `7a5879d...`: PASS, including 113
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-7a5879d.log`, SHA-256
  `af510496105509ea5ac084a0b2528dd42bb4e361870e9b43b4253c6c7a1212c1`.
- Exact-head review then identified a target-qualification P1 and an explicit
  disable ownership P2. `be8b51a...` synchronously publishes the selected B
  target in a `PENDING` pane before old-target cleanup or the B capability
  probe can yield. That pane unmounts every B503 operation, and pending guards
  make stale A listeners inert until B is `AVAILABLE`; the deterministic
  A-available -> B-probing regression clicks stale Enable, Read, and Disable
  controls and proves only B's capability request leaves the browser before B
  returns `NOT_SUPPORTED`. Explicit Disable now captures its submitted token
  and target and clears state only if the same pair remains current. The
  deterministic same-target race proves delayed A disable confirmation retains
  newly enabled B's token/status and a later B disable clears exactly B.
- Focused Portal B503 tests passed 20/20 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `be8b51a...`: PASS, including 115
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-be8b51a.log`, SHA-256
  `2e8a554c78b1d8158b19c24f8fa6d4b8fed562239beba2b343192393d8bd0db9`.
- Exact-head review found that a stale A enable could replace a newer B
  token/target before routing cleanup. `6fc6d8b...` routes token-bound
  disable through one pair-aware helper. A stale enable retains its captured
  pair only when no current token exists; when B is already current, it sends
  a direct A cleanup without publishing A or allowing its confirmed, failed,
  or transport result to change B state/status. The prior no-current retention
  regression remains in place. A deterministic A-enable -> B-active -> late-A
  cleanup `SESSION_BUSY` regression proves the direct request uses A/8 while
  B/21 and the active B status remain recoverable.
- Focused Portal B503 tests passed 21/21 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `6fc6d8b...`: PASS, including 116
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-6fc6d8b.log`, SHA-256
  `514eb6e8a390ad7be5910bfe5b2202aa726bb3b36f62eb8a43c419223f4c3301`.

## Gate boundary

`Project-Helianthus/helianthus-docs-ebus#523` / PR #524 and this repository's
PR #975 remain open. A fresh Gateway exact-HEAD review remains required after
this evidence commit and the accepted docs gate. No live/private action was
performed.
