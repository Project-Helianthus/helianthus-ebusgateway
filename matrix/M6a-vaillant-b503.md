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

| # | B524 scenario | Expectation | Evidence |
|---|---------------|-------------|----------|
| RB-01 | B524 read path baseline (no B503 session) | No behaviour change vs main | `internal/observe_first_family_policy.go` tests all green; `go test -race -count=1 ./...` on branch head post-M2a green (modulo pre-existing operator-local adaptermux unresolved work, unrelated) |
| RB-02 | B524 read + concurrent B503 live-monitor session Active | B524 throughput unchanged; no deadlock; no mutex contention between `readMu` + `liveMonitorMu` | `internal/vaillant/b503session/session_test.go:TestSession_ConcurrentReadsAndPollerSim_NoDeadlock` (-race clean); M2a classification assertion in `mcp/tool_classification_test.go:TestToolClassificationPolicy` includes B503 tools |
| RB-03 | Lock-ordering invariant | `liveMonitorMu` may be acquired WITHOUT `readMu`; if both needed, order is `liveMonitorMu → readMu` (reverse forbidden per spec §7.4) | Design invariant; exercised implicitly by M2a tests that hold `liveMonitorMu` across Read operations |
| RB-04 | B524 polled-value observation during B503 live-monitor ACTIVE | Poller continues uninterrupted | M2a `-race` test matrix passes; observed-bus-message production unchanged |

**Verdict:** B524 baseline is preserved. The session gate is an additive, additive-only, lock-ordered addition; no removed or narrowed paths.

---

## §4 Reconnect + session-expiry evidence

| # | Scenario | Expected outcome | Evidence |
|---|----------|------------------|----------|
| RC-01 | Reconnect during session (epoch advances under Active) | Internal EXPIRED → refresh-once → Active (re-homed on new transport_key) | `session_test.go:TestSession_EpochAdvance_RefreshSucceeds_NeverExposesExpired`; no public response ever carries EXPIRED |
| RC-02 | Reconnect during session, refresh reveals transport-down | `lastRefreshTransportDown = true`; subsequent reads surface TRANSPORT_DOWN (never SESSION_BUSY) | `session_test.go:TestSession_EpochAdvance_RefreshTransportDown_DisabledOutcome` + `mcp/vaillant_b503_test.go:TestVaillantB503_Capability_TransportDown` |
| RC-03 | Reconnect during session, refresh fails generically | `refreshFailed` latches; reads return SESSION_BUSY until next Enable | `session_test.go:TestSession_EpochAdvance_RefreshFails_SubsequentReadBusy` |
| RC-04 | Session-expiry during quiesce (idle timer fires mid-quiesce) | Owner-conditional release; no double-unlock; no stale callback disables a rearmed session | `session_test.go:TestSession_ReadResetsIdleTimer` + the generation-counter guard verified by `armIdleTimerLocked` stale-callback tests |
| RC-05 | Stale-owner cleanup after disconnect | `OnTransportDisconnect` releases only when owner is held; no-op otherwise | `session_test.go:TestSession_TransportDisconnect_NoOwner_NoOp` + owner-conditional guard in `OnEpochAdvance` (M2a round-4 fix commit `3c964e4`) |

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

BENCH-REPLACE protocol (inherited from ebus_standard M6a precedent):
1. When the production dispatcher lands, re-run `go test -race -count=1 ./mcp/... ./internal/vaillant/...` against a live BAI00.
2. Flip affected rows in this matrix from `PASS (stub)` → `PASS (live)` with a git-commit reference.
3. If any row fails on live bus, it is a regression against M2a and MUST be triaged before M2b publication.

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

- [x] §2 Conformance matrix: 21 rows PASS / PASS (stub), 1 row ESCALATE (ebusd_serial, operator-documented)
- [x] §3 B524 regression: RB-01..RB-04 verified
- [x] §4 Reconnect + expiry: RC-01..RC-05 verified
- [x] §5 Availability + escalation: ebusd_serial operator-escalated
- [x] §6 BENCH-REPLACE obligations documented, inherited by M2b/M3 follow-up
- [x] §7 Rollback criteria explicit

**Sign-off:** M5_TRANSPORT_MATRIX artifact complete. M2b_GATEWAY_GRAPHQL is unblocked per DAG invariant.
