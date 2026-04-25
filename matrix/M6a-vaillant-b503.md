# M5 — Vaillant B503 Transport Matrix Artifact

**Milestone:** `M5_TRANSPORT_MATRIX`
**Repo:** `helianthus-ebusgateway`
**Issue:** #517
**Plan:** `helianthus-execution-plans/vaillant-b503-namespace-w17-26.implementing/`
**Predecessors (merged):**
- M0_DOC_GATE — [docs-ebus#283](https://github.com/Project-Helianthus/helianthus-docs-ebus/pull/283) squash `b4cb1c7` (normative B503 spec)
- M1_DECODER — [ebusgo#142](https://github.com/Project-Helianthus/helianthus-ebusgo/pull/142) squash `5491494d` (protocol/vaillant/b503)
- M2a_GATEWAY_MCP — [ebusgateway#516](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/516) squash `d74dc89b` (MCP tools + session FSM + capability signal)

**Blocks (DAG):** M2b_GATEWAY_GRAPHQL — the public consumer contract MUST not publish until this transport gate demonstrates non-regression across adapter-direct / ebusd_tcp.

---

## §1 Purpose + Scope

Per plan §M5_TRANSPORT_MATRIX, this artifact is the **transport gate before public contract publish** for the Vaillant B503 namespace. It enumerates:

1. The conformance grid (transport family × B503 surface × scenario).
2. The B524 regression rows demanded by plan AD12 — the new `liveMonitorMu` mutex MUST NOT perturb existing B524 poll behavior.
3. Reconnect / session-expiry / stale-owner cleanup evidence.
4. Operator escalation note for any transport family unavailable in the lab.

**In scope here:** conformance matrix, B524 regression transcripts, reconnect evidence, rollback criteria, transport-gate sign-off.

**Out of scope here (deliberately):**
- No new session-FSM semantics (M2a is the authoritative design-lock).
- No public GraphQL/Portal surface — those are M2b/M3 and are gated on this matrix.
- No change to the raw-frame stub dispatcher; the M2a-documented M2b/M3 production-dispatcher follow-up is out of scope.

---

## §2 Conformance matrix

Rows cover the (transport × B503 surface × scenario) product. Each cell points to the test file / production wiring that observes the behavior. Status values:

- `PASS` — green on the branch head of this PR.
- `PASS (stub)` — behaviour passes against the M2a stub dispatcher; live-bus re-verification is a BENCH-REPLACE obligation paired with the M2b/M3 dispatcher switchover.
- `ESCALATE` — transport family not reachable in the lab this session; operator decision recorded in §5.

| # | Transport | B503 surface | Scenario | Status | Evidence (test / wiring) |
|---|-----------|--------------|----------|--------|--------------------------|
| VB-01 | adapter-direct | `errors.get` | passive read decode | PASS (stub) | `mcp/vaillant_b503_test.go:TestVaillantB503_ErrorsGet_*` + production stub via `cmd/gateway/vaillant_b503_wiring.go` |
| VB-02 | adapter-direct | `errors.history.get` | index echo + sentinel decode | PASS (stub) | as VB-01 |
| VB-03 | adapter-direct | `service.current.get` | 5-slot service decode | PASS (stub) | as VB-01 |
| VB-04 | adapter-direct | `service.history.get` | indexed history decode | PASS (stub) | as VB-01 |
| VB-05 | adapter-direct | `live_monitor.get` | enable → read → disable lifecycle | PASS | `mcp/vaillant_b503_test.go:TestVaillantB503_LiveMonitor_*` |
| VB-06 | adapter-direct | `live_monitor.get` | second-claimant → SESSION_BUSY | PASS | `mcp/vaillant_b503_test.go` + `internal/vaillant/b503session/session_test.go:TestSession_SecondClaimant_ReturnsBusy` |
| VB-07 | adapter-direct | `live_monitor.get` | wrong issuer_token on disable → SESSION_BUSY / INVALID_ARGUMENT | PASS | `mcp/vaillant_b503_test.go:TestVaillantB503_LiveMonitor_Disable_WrongToken_*` + session tests |
| VB-08 | adapter-direct | `live_monitor.get` | 30s idle auto-disable | PASS | `internal/vaillant/b503session/session_test.go:TestSession_IdleTimeout_AutoDisable` (30ms test timeout; production is 30s) |
| VB-09 | adapter-direct | session FSM | transport disconnect releases lock (owner held) | PASS | `session_test.go:TestSession_TransportDisconnect_OwnerHeld_Releases` |
| VB-10 | adapter-direct | session FSM | transport disconnect no-op (no owner) | PASS | `session_test.go:TestSession_TransportDisconnect_NoOwner_NoOp` |
| VB-11 | adapter-direct | session FSM | gateway restart destroys state | PASS | `session_test.go:TestSession_GatewayRestart_DestroysState` |
| VB-12 | adapter-direct | session FSM | epoch advance → EXPIRED internal → refresh success | PASS | `session_test.go:TestSession_EpochAdvance_RefreshSucceeds_NeverExposesExpired` |
| VB-13 | adapter-direct | session FSM | epoch advance → refresh returns TRANSPORT_DOWN | PASS | `session_test.go:TestSession_EpochAdvance_RefreshTransportDown_DisabledOutcome` |
| VB-14 | adapter-direct | session FSM | epoch advance → refresh fails generically → SESSION_BUSY | PASS | `session_test.go:TestSession_EpochAdvance_RefreshFails_*` |
| VB-15 | adapter-direct | capability | `meta.capabilities.vaillant_b503.reason` reports SESSION_BUSY while held | PASS | `mcp/vaillant_b503_test.go:TestVaillantB503_Capability_*` |
| VB-16 | adapter-direct | envelope | `meta.data_hash` determinism | PASS | `mcp/vaillant_b503_test.go:TestVaillantB503_ErrorsGet_EnvelopeDeterminism` |
| VB-17 | adapter-direct | invariant | `TestNoVaillantInstallWriteTools` (no `*clear*`) | PASS | `mcp/vaillant_b503_test.go:TestVaillantB503_NoInstallWriteTools` |
| VB-18 | adapter-direct | invariant | `TestNoExpiredInPublicResponse` | PASS | `mcp/vaillant_b503_test.go:TestVaillantB503_NoExpiredInPublicResponse` |
| VB-19 | ebusd_tcp | `errors.get` | read path passthrough | PASS (stub) | same handler / dispatcher stub; transport-agnostic decode path |
| VB-20 | ebusd_tcp | `live_monitor.get` | enable/read/disable session lifecycle | PASS (stub) | same session FSM; adapter-agnostic (ownership is transport-key-scoped) |
| VB-21 | ebusd_tcp | invariant | `EXPIRED` never surfaced | PASS | `mcp/vaillant_b503_test.go:TestVaillantB503_NoExpiredInPublicResponse` (transport-agnostic contract) |
| VB-22 | ebusd_serial | all | **ESCALATE** — lab rig not available this session | ESCALATE | see §5 |

### Nota bene on `PASS (stub)` rows

The production raw-frame dispatcher is a deliberate stub until the M2b/M3 dispatcher-bridge lands (see M2a merge commit body). All rows marked `PASS (stub)` exercise the full MCP pipeline — request validation, envelope construction, capability signal, decoder path, error classification — against an in-memory dispatcher. Only the final wire emission is deferred. Once the production bridge ships, these rows MUST be re-run on a live BAI00 to flip from `PASS (stub)` → `PASS (live)`. That is the BENCH-REPLACE obligation inherited from M2a (see §6).

---

## §3 B524 regression transcripts (plan AD12)

M2a introduced `liveMonitorMu` as a dedicated session gate, distinct from the existing B524 `readMu`. Plan AD12 requires explicit evidence that B524 poll behavior is unchanged under this new mutex.

**Honest framing of test coverage.** The adaptermux / B524 `readMu` is package-private; writing in-code regression tests from outside adaptermux would require either (a) an import-cycle-risky dependency or (b) a mock-mutex stand-in that proves nothing about the real production paths. An earlier revision of this PR included proxy tests in `mcp/` that three successive Codex review rounds correctly flagged as weak: they exercised `b503session.Manager` in isolation without touching the actual B524 read path, and a generic CI `-race` run only detects data races, not deadlocks or lock-order cycles. That file was deleted rather than continue to ship theatre.

The authoritative evidence for RB-01..RB-04 therefore lives across three places:

1. **M2a concurrency suite** (`internal/vaillant/b503session/session_test.go`): 14 tests under `-race`, covering FSM transitions, concurrent Enable/Read/Disable, epoch-refresh race guards, idle-timer stale-callback guard. These are the authoritative in-code tests for the `liveMonitorMu`-side of the invariant.
2. **Code review trip-wire**: the comment "`mu sync.Mutex // liveMonitorMu — ownership gate, distinct from B524 readMu.`" on the `b503session.Manager` struct field forces any future refactor that would share or alias the mutex to be explicit about doing so.
3. **Live-bus BENCH-REPLACE** (see §6): once the production raw-frame dispatcher lands (M2b/M3), full-stack B524 + B503 concurrent traffic is observed on live hardware and the regression rows below are ratified.

| # | B524 scenario | Expectation | Evidence |
|---|---------------|-------------|----------|
| RB-01 | B524 read path baseline (no B503 session) | No behaviour change vs main | Existing repo-root `observe_first_family_policy_test.go` (path: `observe_first_family_policy_test.go` at module root) + `cmd/gateway/semantic_vaillant_test.go` B524 test coverage green on branch head; no diff against any `readMu` call site in this PR (verifiable via `git diff origin/main...HEAD -- '*readMu*' '*.go'` — zero lines). |
| RB-02 | B524 read + concurrent B503 live-monitor Active | B524 throughput unchanged; no deadlock between `readMu` + `liveMonitorMu` | `session_test.go:TestSession_ConcurrentReadsAndPollerSim_NoDeadlock` (-race clean) + live-bus BENCH-REPLACE ratification (§6). |
| RB-03 | Lock-order invariant `liveMonitorMu → readMu`; reverse forbidden | Enforced by design — neither mutex is shared; acquisition sites documented in spec §7.4 | Code review trip-wire (struct-field comment) + BENCH-REPLACE. Generic `-race` does NOT prove lock order; that is a distinct class of bug Go's race detector does not catch. |
| RB-04 | B524 polled-value observation during B503 live-monitor Active | Poller continues uninterrupted | Implicit in RB-02 evidence — the existing observation pipeline has zero diff in this PR and the B503 session path is on a distinct mutex. Ratified live-bus via §6. |

**Verdict:** B524 baseline is preserved by construction (zero diff to readMu call sites; distinct mutex; documented lock order). Full in-code regression coverage at production path depth is deferred to live-bus BENCH-REPLACE, consistent with the M2b/M3 dispatcher-switchover milestone.

---

## §4 Reconnect + session-expiry evidence

| # | Scenario | Expected outcome | Evidence |
|---|----------|------------------|----------|
| RC-01 | Reconnect during session (epoch advances under Active) | Internal EXPIRED → refresh-once → Active (re-homed on new transport_key) | `session_test.go:TestSession_EpochAdvance_RefreshSucceeds_NeverExposesExpired`; no public response ever carries EXPIRED |
| RC-02 | Reconnect during session, refresh reveals transport-down | `lastRefreshTransportDown = true`; subsequent reads surface TRANSPORT_DOWN (never SESSION_BUSY) | `session_test.go:TestSession_EpochAdvance_RefreshTransportDown_DisabledOutcome` + `mcp/vaillant_b503_test.go:TestVaillantB503_Capability_TransportDown` |
| RC-03 | Reconnect during session, refresh fails generically | `refreshFailed` latches; reads return SESSION_BUSY until next Enable | `session_test.go:TestSession_EpochAdvance_RefreshFails_SubsequentReadBusy` |
| RC-04 | Session-expiry during quiesce (idle timer fires mid-quiesce) | Owner-conditional release; no double-unlock; no stale callback disables a rearmed session | `session_test.go:TestSession_ReadResetsIdleTimer` + the generation-counter guard verified by `armIdleTimerLocked` stale-callback tests |
| RC-05 | Stale-owner cleanup after disconnect | `OnTransportDisconnect` releases only when owner is held; no-op otherwise | `session_test.go:TestSession_TransportDisconnect_NoOwner_NoOp` + owner-conditional guard in `internal/vaillant/b503session/session.go` `OnEpochAdvance` (grep for `if !m.mutexHeld` inside `OnEpochAdvance` — the mutex-held check after the refresh re-acquires `stateMu`) + `OnTransportDisconnect` (same file, initial `if !m.mutexHeld { return }` guard). Both live on the merged M2a squash `d74dc89b` as reachable files/lines. |

**Verdict:** All reconnect + expiry paths are covered by passing -race tests on head.

---

## §5 Transport availability + operator escalation

| Transport | Status | Note |
|-----------|--------|------|
| adapter-direct | verified against M2a stub + full test matrix | no lab rig required for stub verification |
| ebusd_tcp | verified against M2a stub + transport-agnostic handler / session code | session is transport_key-scoped so code paths do not branch per transport family |
| **ebusd_serial** | **ESCALATE — not available this session** | Operator decision: v1 supported-transports list is narrowed to `{adapter-direct, ebusd_tcp}` for the B503 namespace. `ebusd_serial` transport may be added post-v1 once a lab rig is wired; no code change required (session + decoder are transport-agnostic). This narrowing is recorded here and MUST be cross-referenced in the M2b GraphQL schema changelog when M2b publishes. |

---

## §6 BENCH-REPLACE obligations

The following rows are marked `PASS (stub)` and REQUIRE re-verification on live hardware when the M2b/M3 production raw-frame dispatcher lands. Per the M2a merge commit body: "Raw-frame Dispatcher bridge from adaptermux/router → scheduled M2b/M3".

- VB-01 through VB-04, VB-19, VB-20 — all stub-verified MCP pipeline rows.

BENCH-REPLACE protocol (inherited from ebus_standard M6a precedent). The existing unit-test suites (`mcp/vaillant_b503_test.go` with `stubB503Dispatcher`, `internal/vaillant/b503session/session_test.go` with in-memory FSM) are NOT live-bus evidence — they cannot flip `[~]` rows to `[x]` because they do not exercise any real bus path. Real ratification REQUIRES all of the following:

1. **Operator-attested live bus capture (adapter-direct)** against a BAI00 (or equivalent) physically connected via the adapter-direct transport. Evidence artifact: `matrix/bench-replace/<date>-vaillant-b503-bai00-adapter-direct.log` or equivalent, containing:
   - wire-level request/response bytes for the adapter-direct rows (VB-01..VB-04 plus VB-05 enable/read/disable frames + ACKs) matching the normative selector catalog;
   - concurrent B524 poll traffic visible in the same capture for RB-02/RB-04 ratification on adapter-direct.
2. **Operator-attested live bus capture (ebusd_tcp)** against the same device via the ebusd_tcp transport. Evidence artifact: `matrix/bench-replace/<date>-vaillant-b503-bai00-ebusd-tcp.log`, containing:
   - wire-level bytes for the ebusd_tcp rows (VB-19, VB-20);
   - session-enable/disable evidence for VB-20;
   - concurrent B524 poll traffic for the same RB-02/RB-04 rows (ebusd_tcp side).

Each transport capture only demands rows that are actually executable on that transport; operators cannot produce ebusd_tcp evidence from an adapter-direct run and vice versa.
3. **Gateway-level observation** showing `meta.capabilities.vaillant_b503.reason` transitioned correctly across the session lifecycle during the captures (attach stdout log from the gateway's MCP surface during the run).
4. Only AFTER steps 1–3 have landed as commits in this repo, and the operator has signed off in the follow-up commit body with "BENCH-REPLACE-SIGNOFF: <YYYY-MM-DD> on <device> <transport>", may the affected `[~]` rows be flipped to `[x]`.
5. The gateway unit-test suite MAY be re-run against live hardware in parallel as a smoke check, but its pass/fail alone does NOT satisfy BENCH-REPLACE — the operator capture + attested signoff are the authoritative artifacts.
6. If any row fails live-bus verification, it is a regression against M2a and MUST be triaged before M2b publication; `[~]` stays `[~]`.

Failure to attach the required operator-attested capture on a BENCH-REPLACE commit is a grounds to block that commit's merge.

---

## §7 Rollback criteria

If post-merge, a cruise-run surfaces a regression tied to M2a / this M5 artefact:

- **Symptom class "session double-unlock panic"** → revert M2a commit `d74dc89b` OR land a targeted patch restoring owner-conditional release guard (M2a rounds 4 + 10 commits).
- **Symptom class "EXPIRED leaked to public response"** → verify `TestNoExpiredInPublicResponse` ran on CI for the regressing commit; land `normalizeSessionErr` fix.
- **Symptom class "B524 throughput regression under concurrent B503 live-monitor"** → revert `liveMonitorMu`-adjacent changes; the dual-mutex design is deliberately isolating, so any throughput perturbation is a bug, not a design tradeoff.
- **Symptom class "capability.reason contradicts error.code"** → regression against M2a round 10 (`IsOwned()` check) — land the check back in `VaillantB503AvailabilityCtx`.

Escalation threshold: any of the above reproducing on main → **immediate** revert to `M1_DECODER` merge state (`5491494d`'s dependent gateway-side commits) is acceptable; public GraphQL contract (M2b) MUST NOT publish until the issue is resolved + a new M5 matrix commit lands.

---

## §8 Transport-gate sign-off

The sign-off below uses two checkbox states:

- `[x]` = **stub-verified / documented / static-guaranteed** — the row's claim holds against the M2a stub-dispatcher test matrix, the artifact structure is present, or the invariant is static (no diff to relevant call sites).
- `[~]` = **pending live-bus BENCH-REPLACE** — the row's full production-path coverage is deferred to live-hardware verification when the M2b/M3 raw-frame dispatcher lands (§6).

| # | Section | State | Note |
|---|---|---|---|
| 1 | §2 Conformance matrix — 21 rows PASS / PASS (stub), 1 ESCALATE (ebusd_serial) | `[x]` stub-verified; PASS (stub) rows carry forward to BENCH-REPLACE per §6 |
| 2 | §3 RB-01 (B524 baseline, no B503 session) | `[x]` static: zero diff to `readMu` call sites in this PR |
| 3 | §3 RB-02 (B524 + concurrent B503 Active, real paths) | `[~]` pending BENCH-REPLACE — stub-mutex proxy deleted after honest review; production-path evidence requires live bus |
| 4 | §3 RB-03 (lock-order invariant) | `[~]` pending BENCH-REPLACE — generic `-race` does NOT catch this bug class; static coverage is the M2a struct-field trip-wire + code review |
| 5 | §3 RB-04 (B524 poll during B503 Active) | `[~]` pending BENCH-REPLACE — subsumed by RB-02 on live bus |
| 6 | §4 Reconnect + expiry RC-01..RC-05 | `[x]` verified by M2a `session_test.go` under `-race` |
| 7 | §5 Availability + ebusd_serial escalation | `[x]` documented |
| 8 | §6 BENCH-REPLACE obligations | `[x]` inherited by M2b/M3 follow-up |
| 9 | §7 Rollback criteria | `[x]` explicit |

**Sign-off state:** **conditional — artifact-complete, live-bus-pending**. M5_TRANSPORT_MATRIX is complete AS AN ARTIFACT; the plan-AD09 DAG invariant ("`M5 blocks M2b`") is satisfied at the **artifact level** — M2b_GATEWAY_GRAPHQL may proceed to **implementation** (decoder wiring, resolver plumbing, test-time dispatcher stub).

**M2b publication (schema-stable v1) MUST NOT land before every `[~]` row in this sign-off flips to `[x]`.** This is not a recommendation. The header of this artifact states that the public consumer contract "MUST not publish until this transport gate demonstrates non-regression across adapter-direct / ebusd_tcp" — RB-02, RB-03, RB-04 are part of that non-regression demonstration, and their live-bus evidence is the missing prerequisite.

Concrete release-gate rule, unambiguous:
1. M2b implementation PR may open any time after M5 merges.
2. M2b implementation PR may merge (land the code) any time after M5 merges.
3. M2b **schema-stable publication** — announcing the v1 GraphQL surface as stable to consumers, adding it to the schema changelog, or cutting a release — **MUST NOT** occur until RB-02/RB-03/RB-04 flip to `[x]` via the BENCH-REPLACE procedure in §6 and this artifact receives a follow-up commit that records the flip.
4. If M2b implementation ships code that would de-facto stabilize the schema (e.g., public docs referencing v1 fields, HA integration consuming v1), that is equivalent to publication and is equally gated.

This explicit separation prevents the matrix from falsely advertising live-bus coverage that has not yet been performed, and removes the SHOULD/MUST ambiguity that would let schema-stable publication slip ahead of live-hardware validation.

---

## §9 Production-dispatcher rows (M6_DISPATCHER_BRIDGE)

Per amendment-1 plan §M6 acceptance and helianthus-docs-ebus B503.md §12, the production raw-frame dispatcher (`*rawFrameDispatcher` in `cmd/gateway/vaillant_b503_dispatcher.go`) replaces the M2a `b503StubDispatcher{}` injection. Routing is single-substrate through `gw.Bus` (`*protocol.Bus`) — the same path B524/B525 already use (AD16). No parallel transport.

Status values for §9:

- `[bridge-PASS]` — green against the mocked transport coverage delivered with M6. The dispatcher routes through `bus.Send`, the error-mapping table (§12.4) is enforced, the AD18 stale-epoch discipline is exercised, and the AD16 lock-order invariant is mechanically verified by the `//go:build raceconcurrency` tracer suite.
- `[bridge-LIVE-PASS]` — flips to this state when M7_BENCH_REPLACE attaches operator-attested live-bus captures per §6 BENCH-REPLACE protocol. Until then `[bridge-PASS]` is the truthful status (mocked-transport, code-path complete).

The 5 read selectors + 3 live-monitor lifecycle phases + 1 mixed-traffic regression × 2 transport families = 16 rows. M6 lands all 16 as `[bridge-PASS]`; M7 flips them to `[bridge-LIVE-PASS]` after capture artefacts land in `matrix/captures/`.

| # | Transport | Surface | Scenario | Status | Evidence |
|---|-----------|---------|----------|--------|----------|
| VB-BR-01 | adapter-direct | `errors.get` | dispatch via `bus.Send`; PB=B5 SB=03 envelope | `[bridge-PASS]` | `cmd/gateway/vaillant_b503_dispatcher_test.go:TestM6Dispatcher_ErrorsCurrent_RoutesViaBusSend` |
| VB-BR-02 | adapter-direct | `errors.history.get` | dispatch with index byte | `[bridge-PASS]` | `:TestM6Dispatcher_ErrorsHistory_RoutesViaBusSend` |
| VB-BR-03 | adapter-direct | `service.current.get` | dispatch | `[bridge-PASS]` | `:TestM6Dispatcher_ServiceCurrent_RoutesViaBusSend` |
| VB-BR-04 | adapter-direct | `service.history.get` | dispatch with index byte | `[bridge-PASS]` | `:TestM6Dispatcher_ServiceHistory_RoutesViaBusSend` |
| VB-BR-05 | adapter-direct | `live_monitor.get` (00 03) enable | dispatch under SERVICE_WRITE class | `[bridge-PASS]` | `:TestM6Dispatcher_LiveMonitor_RoutesViaBusSend` + `:TestM6Conc01_DisconnectDuringEnableHandshake` (`-tags=raceconcurrency`) |
| VB-BR-06 | adapter-direct | `live_monitor.get` (00 03) read | steady-state read returns Frame.Data verbatim | `[bridge-PASS]` | `:TestM6Conc02_DisconnectDuringSteadyStateRead` |
| VB-BR-07 | adapter-direct | `live_monitor.get` disable | idempotent under disconnect | `[bridge-PASS]` | `:TestM6Conc03_DisconnectDuringDisable` |
| VB-BR-08 | adapter-direct | mixed B524 + B503 | concurrent traffic, no stale-epoch leak | `[bridge-PASS]` | `:TestM6Conc04_ReconnectUnderConcurrentTraffic_NoStaleEpochLeak` (8-row truth-table row 8) |
| VB-BR-09 | ebusd_tcp | `errors.get` | dispatch through same code path; transport-agnostic | `[bridge-PASS]` | shared `*rawFrameDispatcher` — transport branch is at `protocol.Bus.Send`; tests cover the dispatcher boundary uniformly |
| VB-BR-10 | ebusd_tcp | `errors.history.get` | as above | `[bridge-PASS]` | shared (transport-agnostic dispatcher) |
| VB-BR-11 | ebusd_tcp | `service.current.get` | as above | `[bridge-PASS]` | shared |
| VB-BR-12 | ebusd_tcp | `service.history.get` | as above | `[bridge-PASS]` | shared |
| VB-BR-13 | ebusd_tcp | `live_monitor.get` enable | as above | `[bridge-PASS]` | shared |
| VB-BR-14 | ebusd_tcp | `live_monitor.get` read | as above | `[bridge-PASS]` | shared |
| VB-BR-15 | ebusd_tcp | `live_monitor.get` disable | as above | `[bridge-PASS]` | shared |
| VB-BR-16 | ebusd_tcp | mixed B524 + B503 | concurrent traffic, AD16 lock order | `[bridge-PASS]` | shared lock-tracer suite + concurrency tests |

**Honest framing preserved.** §3 already records that the `-race` trip-wire is SECONDARY proof; the M6 lock tracer (`//go:build raceconcurrency` in `vaillant_b503_dispatcher_concurrency_test.go`) is the primary mechanical verification of AD16 / §12.6. M6 does NOT dilute the §3 framing — it adds an additional trip-wire on top of the existing static and `-race` coverage. The "live-bus" gap remains: M6 mocked transports pass `bus.Send` byte streams matching `LOCAL_CAPTURE`; M7 supplies operator-attested wire captures.

**Cross-reference to §3 RB rows.** §9 production-dispatcher rows do NOT supersede the §3 RB-02 / RB-03 / RB-04 `[~]` markers — those concern B524 baseline non-regression under live load and remain pending BENCH-REPLACE per the §3 honest framing. The two row sets answer different questions: §3 RB asks "does B524 throughput regress under B503 load on real hardware"; §9 VB-BR asks "does the production dispatcher's code path correctly serialise frame envelope, error mapping, lock order, and stale-epoch discipline against the agreed-upon test contract".

**Forward gate.** When M7_BENCH_REPLACE merges with operator-attested capture artefacts, every `[bridge-PASS]` row flips to `[bridge-LIVE-PASS]` and §3 RB-02/03/04 simultaneously flip from `[~]` to `[x]`. That is the only authorised promotion path for the dispatcher rows — flipping any §9 row without the corresponding capture is a defect.
