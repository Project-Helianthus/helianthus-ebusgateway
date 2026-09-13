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
- Exact-head review found that projection-card navigation published B directly
  without the target-switch pending fence, and that invalid capability target
  parsing could null an otherwise valid GraphQL query. `eb9478d...` factors
  synchronous target qualification into one path used by both the picker and
  projection cards. Card clicks now publish B `PENDING`, withdraw old controls,
  avoid a duplicate activation probe, then run cleanup/probe in order. The
  deterministic A-available -> click-B-card -> B-NOT_SUPPORTED regression
  clicks stale Enable/Read/Disable controls and proves no B live operation is
  dispatched. `vaillantCapabilities` is now a nullable root while its returned
  capability child fields remain non-null. The mixed-root regression sends
  target 256, receives the structured `INVALID_ARGUMENT`, observes only that
  root as null, and retains its valid `vaillantErrors` sibling. The public
  runtime-provider contract and schema characterization digest record the
  deliberate root nullability.
- Focused Portal B503 tests passed 22/22 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `eb9478d...`: PASS, including 117
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-eb9478d.log`, SHA-256
  `6c5054a71c4bc96f118ad8e9db50632c97efd667f2fe20b896ad37648cbba6a6`.
- Exact-head review found two remaining async ownership gaps. `275c75c...`
  makes the shared synchronous target-qualification entry return its captured
  epoch/target. Picker and card paths now proceed past awaited cleanup only
  when that exact context remains current, and the capability probe consumes
  that captured context instead of rereading newer state. The deterministic
  A->B->C regression delays A cleanup, lets C qualify, then completes B's
  cleanup and proves C is the single capability request/result. Explicit
  Disable now handles a stale response specially: when its submitted token and
  target are still the held pair, a confirmed `disabled: true` clears them even
  after a target-epoch change; a replacement pair still remains protected. The
  deterministic pending Disable-A plus target switch makes concurrent nav-away
  cleanup return `SESSION_BUSY`, then confirms explicit A cleanup clears its
  otherwise unusable recovery pair.
- Focused Portal B503 tests passed 24/24 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `275c75c...`: PASS, including 119
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-275c75c.log`, SHA-256
  `f1d3aab8e1cdba6690e7a353278026a7f407bca5e651520c4af00c17daeb24be`.
- Exact-head review found that the stable bounded history list discarded a
  verified prefix whenever a later indexed read failed. `1112637...` changes
  the stable MCP data contract to `{records, failure}`. `records` is the
  ascending verified prefix; nullable `failure {index, code, message}` names
  the first failed native read, and no later index is attempted or fabricated.
  The MCP output/tool goldens and deterministic data-hash test cover this
  shape. GraphQL now projects the same typed result with non-null records and
  failure children when present, so partial failure remains data rather than
  nulling the root. Production MCP-to-GraphQL parity covers the complete and
  partial paths. Portal renders a first-load prefix with its warning and, on a
  later partial/network failure, merges verified updates into its current
  target/epoch cache rather than replacing a valid table wholesale. The public
  runtime-provider contract and schema-characterization digest record this
  stable partial-result behavior.
- Focused Portal B503 tests passed 26/26 and focused GraphQL/MCP/gateway
  `go test -race` passed. Final
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `1112637...`: PASS, including 121
  Portal Node tests, repository-wide race tests, source schema validation,
  Python 168+6+26+11+6+2, zero lint findings, and green Storage/EVSE SemReg
  mapping gates. Transport and passive-smoke were correctly not triggered.
  Durable log: `ci_local-final-1112637.log`, SHA-256
  `e0391a81eda06755da9ee69603d4edc248d947cecb748988f1eadd99752e69a4`.


- Exact-head P2 `PRRT_kwDORGIw3c6h5TmQ` found a contradictory observation during
  `OnEpochAdvance`: the internal refresh transition exposed `Disabled` while
  retaining the live-monitor ownership gate. `f03619b...` replaces that
  ambiguity with the stable public `Refreshing` state. `StatusSnapshot` now
  captures `Refreshing, owned:true` atomically; `Read`, `Disable`, and a second
  `Enable` return `SESSION_BUSY` while it is held. Successful refresh returns
  `Active`; transport and other failures retain the existing release-to-`Idle`
  behavior. MCP uses that snapshot, GraphQL exposes a closed
  `B503SessionState` enum including `Refreshing`, and the Portal strip both
  states that the gate is held and disables Enable, Read, and Disable.
  Deterministic blocked-refresh manager, MCP, GraphQL, MCP-to-GraphQL parity,
  and browser regressions cover the transitional state and terminal success.
- The runtime-provider contract now records the five stable session labels and
  the invariant that `Disabled` never has `owned: true`. The GraphQL
  characterization digest is `63b8df876d8e00d98c26d4071d4ec532edc28ff07aa7c30aca0e55fa32f2ff28`.
  The pre-existing idle golden remains byte-identical and the new deterministic
  `vaillant_b503_live_monitor_session_refreshing.golden.json` freezes the
  `Refreshing` state, ownership, `SESSION_BUSY` capability metadata, and stable
  data hash.
- Required companion wording for docs-ebus #524, without editing that
  repository: **“The gateway-owned B503 session strip has five stable states:
  `Idle`, `Enabling`, `Active`, `Refreshing`, and `Disabled`. `Refreshing`
  means an epoch refresh still holds the ownership gate and all live-monitor
  operations are busy; success returns `Active`, refresh failure releases the
  gate and returns `Idle`, and `Disabled` is never reported with `owned: true`.”**
- Focused validation: `node --test portal/web/test/vaillant-b503.test.mjs`:
  PASS, 27 tests. `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off go test -race ./internal/vaillant/b503session ./mcp ./graphql
  ./cmd/gateway -run 'Test(Session|State|VaillantB503|Issue552VaillantB503|QuerySchema)' -count=1`:
  PASS.
- Final `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on source `f03619b...`: PASS, including 122
  Portal Node tests, repository-wide `go test -race`, Python
  168+6+26+11+6+2, zero `golangci-lint` issues, Modbus RTU transport
  conformance, and green Storage/EVSE SemReg mapping gates. Passive smoke was
  correctly not triggered. Durable log: `ci_local-final-f03619b.log`, SHA-256
  `1e41c2ffcedf5b238daa337d49111178ec24f3bf7f89100282e702697c0d2331`.


- The final golden correction is source commit `35c41c9...` (tree
  `c306f9bb35ad5a5d33fb6e4d63029e46cf24168e`). Its complete
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` run passed: Portal 122 tests,
  repository-wide `go test -race`, Python 168+6+26+11+6+2, zero lint issues,
  Modbus RTU transport conformance, and the Storage/EVSE SemReg gates; passive
  smoke was not triggered. Durable log: `ci_local-final-35c41c9.log`, SHA-256
  `405bf9731a64c307e81e5c0324ed57a13dcfe33c77183385df018e21a6f1996f`.


- Independent exact-head review `gateway975-0eb0124-final-independent/REPORT.md`
  (SHA-256 `2dbc1215ed1e25e956c6370913cfd633eb93d44183d9f9e0e3c0847e602c378d`)
  found P2 `PRRT_kwDORGIw3c6h5aoM`: target/card navigation passed its previous
  picker target into cleanup even when the retained issuer token was paired
  with a different target. `59f9604...` makes navigation snapshot only the
  held token and `_vaillantB503LiveTarget` together before awaiting disable;
  the historical picker/card target cannot substitute for that recovery pair.
  Confirmed closure therefore clears exactly the matching pair and preserves
  the newly selected target.
- Deterministic Portal regressions cover retained A/8 while B->C runs through
  the picker and a retained configured-default (`null`) token while B->C runs
  through a projection card. They prove emitted disable variables use A/8 and
  `null` respectively, confirmation clears only the retained pair, and C stays
  selected. Existing Refreshing behavior and all earlier ownership guards remain
  unchanged.
- Focused validation: `node --test portal/web/test/vaillant-b503.test.mjs`:
  PASS, 29 tests. `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off go test -race ./internal/vaillant/b503session ./mcp ./graphql
  ./cmd/gateway -run 'Test(Session|State|VaillantB503|Issue552VaillantB503|QuerySchema)' -count=1`:
  PASS. Final `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `59f9604...`: PASS, including Portal
  124, repository-wide `go test -race`, Python 168+6+26+11+6+2, zero lint
  issues, Modbus RTU transport conformance, and Storage/EVSE SemReg gates.
  Passive smoke was correctly not triggered. Durable log:
  `ci_local-final-59f9604.log`, SHA-256 `9fe6ea29824a5d196a837ddd58d37d8861bcd49b55615210933e71914b1f2a0d`.


- Exact-head P2 `PRRT_kwDORGIw3c6h5iU9` found that the visible Portal
  Live-Monitor strip could remain stale after idle expiry, another client change,
  or a failed local action. `aef8f57...` adds a bounded five-second read-only
  session refresh only while the B503 live tab is visible; it stops on tab or
  section exit, document hiding, and component disconnect, retaining the
  existing target/epoch fence and adding no write.
- Compatible docs524 refinement in `95bebcd...`: while a locally owned session
  is `Refreshing`, the ownership strip remains observable even if capability is
  temporarily unknown, with no operations or projection card exposed. A
  target/nav cleanup rejected during Refreshing retains its token-target pair,
  retries once only after `Active`, and clears the pair locally if the refresh
  instead releases to `Idle` or `Disabled`.
- Fake-timer Portal regressions cover external idle transition, hidden/nav timer
  cleanup, failed-action refresh, Refreshing visibility under unknown capability,
  deferred exact-pair cleanup after Active, and release without an unnecessary
  cleanup write. Focused Portal validation: PASS, 35 tests. Focused B503 Go
  `-race` suites: PASS. Final `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` on `95bebcd...`: PASS, including Portal 130,
  repository-wide race, Python 168+6+26+11+6+2, zero lint, Modbus RTU transport,
  and Storage/EVSE SemReg gates; passive smoke not triggered. Durable log:
  `ci_local-final-95bebcd.log`, SHA-256 `f2da6ebf0636f77d766c806df6ea1470b04791d76f74d6e62778fb19bb5140a5`.


- Exact-head P2s `PRRT_kwDORGIw3c6h5rxh` and `PRRT_kwDORGIw3c6h5rxj`
  are corrected by source commit `eeee737e3635fe4d83c8aa4c465f02148cb03daf`
  (tree `4280d64c6cb09d743eb6d9d2fe5b498a14c07d7d`). When nav/tab exit
  encounters a locally owned `Refreshing` session, Portal now keeps the
  captured issuer-token/target pair and starts a distinct five-second,
  read-only status task. It is independent of visible-tab polling, issues at
  most one cleanup write after `Active`, clears only after `Idle`/`Disabled`
  release or `disabled:true`, and stops on pair replacement, three status
  failures, twelve non-terminal status reads, or component disconnect. A
  disconnect retains the pair and reconnect resumes the bounded read task.
- Session GraphQL envelopes with `errors`, a null root, or malformed state are
  refresh failures. Portal leaves the last valid state/ownership and exact
  retained token-target pair intact; it never infers `Unknown, owned:false` or
  clears a session from that response. The public runtime-provider contract
  records these bounded cleanup and error semantics.
- Deterministic fake-timer Portal regressions prove hidden/nav-away
  `Refreshing -> Active` cleanup uses the captured A/8 pair exactly once,
  GraphQL error/null preservation and finite failure termination without a
  write, finite non-terminal `Refreshing` termination, and disconnect timer
  shutdown without discarding the recovery pair. Focused
  `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 39 tests.
- Complete `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh`: PASS, including Portal 134 tests,
  repository-wide `go test -race`, Python 168+6+26+11+6+2, zero
  `golangci-lint` issues, Modbus RTU transport conformance, Storage/EVSE
  SemReg gates, and a correctly non-triggered passive smoke gate. Durable log:
  `ci_local-final-deferred-cleanup.log`, SHA-256
  `da796579a0901a97bb7f40dfa8772a142477fbea3d91235ef976eda95a200d7d`.


- Follow-up source commit `0e49b3019bfbfc438b1141a08f6c2872a2e21142`
  (tree `28030ec72ab9d48dfc219e9f3371ce42f840ac9c`) closes the remaining
  component-lifecycle race in the same P2 correction. A detached deferred
  status task now exits before its request and rechecks attachment after its
  await, so a late `Active` response after disconnect cannot emit a hidden
  cleanup write. The exact retained token-target pair and deferred recovery
  state remain available if that component reconnects.
- The deterministic fake-timer regression starts a status read, disconnects the
  component before the `Active` result resolves, and proves zero disable writes
  with the pair still retained. Focused
  `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 40 tests.
  The complete `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh` run passed: Portal 135 tests,
  repository-wide `go test -race`, Python 168+6+26+11+6+2, zero lint issues,
  Modbus RTU transport conformance, Storage/EVSE SemReg gates, and passive
  smoke correctly not triggered. Durable log:
  `ci_local-final-disconnect-fence.log`, SHA-256
  `80efccf09fd506269c0332526e6587b99d0a80c6a9171e4a1b4af8980eef4a2e`.


- Exact-head P2s `PRRT_kwDORGIw3c6h5zjz` and `PRRT_kwDORGIw3c6h5zj5`
  are corrected by source commit `9c1324ecf55597d26dc7da7e33af0468e14fc8be`
  (tree `e14a73f666d204ea5b246ebb6622d282912e38c8`). Target/card navigation
  still synchronously publishes target-bound `PENDING`, then detaches the
  old-pair cleanup and yields once before qualification. A never-settling old
  cleanup therefore cannot block B/C capability qualification; repeated
  selections reuse one in-flight cleanup for the same retained token-target
  pair and do not issue duplicate writes.
- Visible and deferred session reads now enter one serialized request slot.
  Every status intent carries a monotonically advancing version, so a newer
  target, epoch, visibility, or lifecycle change suppresses an older result
  before state/UI mutation or deferred cleanup. The latest queued read runs
  only after the slow predecessor releases the slot.
- Deterministic Portal regressions cover a never-resolving A cleanup followed
  by B/C qualification with one cleanup request, and a slow visible status
  read superseded by deferred status: maximum one request in flight, no stale
  `Idle` overwrite, and exactly one confirmed cleanup write. Focused
  `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 42 tests.
- Complete `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
  GOWORK=off ./scripts/ci_local.sh`: PASS, including Portal 137 tests,
  repository-wide `go test -race`, Python 168+6+26+11+6+2, zero
  `golangci-lint` issues, Modbus RTU transport conformance, Storage/EVSE
  SemReg gates, and a correctly non-triggered passive smoke gate. Durable log:
  `ci_local-final-serialized-session.log`, SHA-256
  `fdef53a393497a719f2468022516ac3c68bffe76a51d6f681caf79cc18d5d5cc`.


- Exact-head P2s `PRRT_kwDORGIw3c6h52nk`, `PRRT_kwDORGIw3c6h52nm`, and
  `PRRT_kwDORGIw3c6h52nn` are corrected by source
  `4bdb42000fe8b3f4a582327a7df0e59b0a0438e9` (tree
  `d619f051989e5347381ec602e838aa5e6d927c3d`). Optional empty-plane B503
  card probes are detached from projection/bootstrap completion; late device
  discovery rerenders only the active target picker against its existing
  capability state; and history retention is a per-resolved-target map while
  epoch continues solely as a stale-response fence.
- Deterministic Portal regressions cover stalled optional probe completion,
  late discovery without unqualified actions, and A complete -> B -> A partial
  history retention. Focused `node --test portal/web/test/vaillant-b503.test.mjs`:
  PASS, 45 tests. Complete configured CI: PASS, Portal 140 tests,
  repository-wide `go test -race`, Python 168+6+26+11+6+2, zero lint issues,
  Modbus transport, Storage/EVSE gates, and passive smoke not triggered.
  Durable log `ci_local-final-projection-history.log`, SHA-256
  `833cdabd5f5b6c4b18191f5681fbef3216c91b51a597c42cfa64f6a98251ee80`.

## Gate boundary

### Final authoritative deferred-cleanup correction

Exact-head P2s `PRRT_kwDORGIw3c6h573v` and
`PRRT_kwDORGIw3c6h573y` are corrected in the Portal source and generated
asset. An unconfirmed disable now obtains one serialized, read-only
`vaillantLiveMonitorSession` result for the captured token-target pair. Only a
valid `Refreshing, owned:true` result starts the existing bounded deferred
status loop; GraphQL errors, null/malformed roots, stale requests, pair
replacement, and disconnect retain the pair and never cause a cleanup write.
The tab click detaches cleanup before changing from Live Monitor, so a stalled
disable cannot delay Errors, Service, or History selection.

Focused browser validation is `node --test portal/web/test/vaillant-b503.test.mjs`:
PASS, 47 tests. New deterministic cases prove cached `Active` cannot suppress
authoritative `Refreshing` recovery, exactly one status read and one disable
write occur, a hung disable still selects Errors immediately, and the retained
pair remains isolated. Existing regressions retain the finite status-failure
and non-terminal budgets, no duplicate/late write behavior, and target,
epoch, and disconnect fences.

Complete validation:
`SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off ./scripts/ci_local.sh`:
PASS. It includes Portal 142, repository-wide `go test -race`, Python script
tests, zero `golangci-lint` issues, Modbus RTU production conformance, and
Storage/EVSE SemReg mapping gates; passive smoke was not triggered. Durable
log: `ci_local-final-authoritative-deferred-cleanup.log`.

### Bounded serialized session-status correction

Exact-head P2 `PRRT_kwDORGIw3c6h5__E` is corrected by making every serialized
session-status read abortable and time-bounded. Invalidating a target,
visibility, or component generation aborts and releases its current logical
slot immediately; a five-second timeout also releases a transport that ignores
abort. A late response is detached from the logical request, so it cannot
update state, clear the retained token-target pair, or emit a cleanup write.

Deterministic Portal tests cover a never-settling status fetch timing out before
the next generation reads Active, and component disconnect aborting a stalled
request before reconnect succeeds. They assert timeout-handle cleanup, aborted
signal delivery, retained pair state, and zero late cleanup writes. Focused
`node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 49 tests.

Complete `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
GOWORK=off ./scripts/ci_local.sh`: PASS, with Portal 144, repository-wide race
tests, Python script tests, zero lint findings, Modbus RTU conformance,
Storage/EVSE SemReg gates, and non-triggered passive smoke. Durable log:
`ci_local-final-bounded-session-status.log`.

`Project-Helianthus/helianthus-docs-ebus#523` / PR #524 and this repository's
PR #975 remain open. A fresh Gateway exact-HEAD review remains required after
this evidence commit and the accepted docs gate. No live/private action was
performed.
