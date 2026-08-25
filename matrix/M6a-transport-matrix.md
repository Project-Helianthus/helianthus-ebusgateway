# M6a — Live-Bus Transport Matrix Artifact

**Milestone:** M6a_transport_matrix_artifact
**Status:** AUTHORED (matrix + rule-7 golden landed) / WAITING (BENCH-REPLACE)
**Repo:** helianthus-ebusgateway
**Issue:** #513
**Predecessors (all merged):** M4B semantic lock, M4D responder capability lock, M4c2 gateway responder runtime (`meta.capabilities.responder` @ minor=1), M5_PORTAL consumer UI, M5b HA no-op compat.
**Plan reference:** `helianthus-execution-plans/ebus-standard-l7-services-w16-26.implementing/00-canonical.md` — M6a row.

---

## §1 Purpose + Scope

M6a is the deployment-grade proof milestone for the ebus-standard L7 services cruise run. It is **not** a new feature milestone; it is the conformance-artifact milestone that (a) enumerates the offline and live-bus signals that constitute "we did not regress", (b) catalogs where those signals are observed (test file references), and (c) declares per-repo rollback criteria should a later cruise run surface a regression tied to M4/M5 work.

M6a is also the vehicle for folding in the M4D ch.13 §10.3 producer-side follow-up: a forward-compat golden pairing an unknown `active.transport` with a known non-`none` `active.scope` (rule-7 scope-override precedence). That follow-up is landed in this PR as a sibling golden + 4 tests, see §2 row FC-7 below.

**In scope here:** conformance matrix, regression-transcript catalog, rollback criteria, BENCH-REPLACE carry-forward documentation.

**Out of scope here (deliberately):**
- No change to the `15 * time.Millisecond` responder ACK placeholder (decision doc §7.1(1) requires a BASV2 live-bus rig; no rig available this session).
- No modification to `forward_compat_synthetic_v1_1.golden.json` (M4c2-locked).
- No modification to decision doc §7.1 (historical record).
- No modification to M4D ch.13 (merged normative).

---

## §2 Conformance matrix

Rows cover the (transport × L7 surface × scenario) product at contract minor=1. Each cell points to the test file that observes the behavior; "status" is the authoritative conformance state as of M4c2 + M5_PORTAL merge.

| # | Transport | L7 surface | Scenario | Status | Evidence (test / golden) |
|---|-----------|------------|----------|--------|--------------------------|
| CM-01 | ENH | `services.list` | happy-path determinism | PASS | `mcp/ebus_standard/golden_test.go` + `testdata/services_list.golden.json` |
| CM-02 | ENH | `commands.list` | PB-03 filter coverage | PASS | `mcp/ebus_standard/golden_test.go` + `testdata/commands_list_pb03.golden.json` |
| CM-03 | ENH | `command.get` | known-id (`alpha`) | PASS | `mcp/ebus_standard/golden_test.go` + `testdata/command_get_alpha.golden.json` |
| CM-04 | ENH | `decode` | known payload (`alpha`) | PASS | `mcp/ebus_standard/golden_test.go` + `testdata/decode_alpha.golden.json` |
| CM-05 | ENS | `services.list` | delegation-parity | PASS (inherited) | Delegation via shared envelope composer; `mcp/ebus_standard/server_test.go` covers composer. |
| CM-06 | ENS | `commands.list` | delegation-parity | PASS (inherited) | as CM-05 |
| CM-07 | ENS | `command.get` | delegation-parity | PASS (inherited) | as CM-05 |
| CM-08 | ENS | `decode` | delegation-parity | PASS (inherited) | as CM-05 |
| CM-09 | ebusd-tcp | read-only L7 paths | legacy read surfaces | PASS | `cmd/gateway/wiring_test.go` (ebusd alias canonicalisation); responder emission blocked per `responder_capability_test.go`. |
| CM-10 | all | any FF-responder surface | policy-denied path | PASS | `internal/execution_policy/policy_test.go` (`RequestOrResponseRole = RoleResponder` + `transport_capability_requirements`). |
| FC-1 | synthetic v1.1 | envelope | unknown `transports[].state` | PASS | `mcp/ebus_standard/forward_compat_test.go` + `testdata/forward_compat_synthetic_v1_1.golden.json` |
| FC-2 | synthetic v1.1 | envelope | unknown `transports[].reason` | PASS | as FC-1 |
| FC-3 | synthetic v1.1 | envelope | unknown `active.scope` | PASS | as FC-1 (rule 6) |
| FC-7 | synthetic v1.1 | envelope | **unknown `active.transport` + known non-`none` `active.scope` (rule 7)** | PASS (new, this PR) | `mcp/ebus_standard/forward_compat_unknown_active_transport_test.go` + `testdata/forward_compat_unknown_active_transport_v1_1.golden.json` |
| CM-11 | all | envelope | `meta.data_hash` determinism | PASS | `mcp/ebus_standard/envelope_test.go` (canonical-JSON + SHA-256) |
| CM-12 | all | envelope | `contract.minor = 1` bump | PASS | `mcp/ebus_standard/responder_capability_test.go` |

**Nota bene.** Rows FC-1 through FC-7 enumerate the seven forward-compat consumer rules from M4D ch.13 §4. M4D §10 mandates these exist "continuously"; this matrix is the index.

---

## §3 Vaillant 0xB5 regression transcripts

The gateway semantic layer decodes Vaillant-proprietary 0xB5 framing for five device families (BAI00, BASV2, VR_71, SOL00, NETX3 — memory-root constants). M4/M5 work did not touch the 0xB5 decoder path; the following tests MUST remain green as a gate on M6a sign-off.

| Device family | Coverage area | Evidence |
|---------------|---------------|----------|
| BAI00 (heat source, 0x08) | semantic snapshot / boiler status | `cmd/gateway/semantic_vaillant_test.go`, `cmd/gateway/semantic_vaillant_adapter_info_test.go` |
| BASV2 (regulator, 0x15) | zones / DHW / cylinders / solar delegation | `cmd/gateway/semantic_vaillant_test.go`, `cmd/gateway/semantic_cache_test.go` |
| VR_71 (controller / FM5, 0x26) | FM5 mode + schedules | `cmd/gateway/semantic_vaillant_test.go` |
| SOL00 (stealth, 0xEC) | solar signal | `cmd/gateway/semantic_vaillant_test.go` |
| NETX3 (radio / 0x04,0xF6) | identification + adapter info | `cmd/gateway/startup_scan_test.go`, `cmd/gateway/startup_scan_diag_test.go`, `cmd/gateway/semantic_vaillant_adapter_info_test.go` |

Cross-repo: `helianthus-ebusreg` owns the B524 dual-namespace opcode decoding that the gateway delegates to. Its golden fixtures are the lower-level ground truth; gateway tests pin the semantic-layer projection.

---

## §4 NM wire regression

The NM (network manager) stream has cadence + framing invariants that are independent of the L7 catalog but are load-bearing for responder role (FF 03-06 surfaces). M4/M5 work did not retune the NM cadence; the following tests MUST remain green.

| Area | Invariant | Evidence |
|------|-----------|----------|
| NM runtime | stream wire framing stability | `internal/nm_runtime/nm_runtime_test.go` |
| NM runtime | catalog integration (surface ↔ NM mapping) | `internal/nm_runtime/nm_runtime_catalog_integration_test.go` |
| NM responder | FF 03-06 responder acceptance / refusal | `internal/nm_runtime/responder_runtime_test.go` |
| Adversarial | scenario replay / fault injection | `internal/adversarial/adversarial_test.go` |

---

## §5 07 FF cadence floor

The 07 FF NM cadence contract is a merged normative obligation. M6a references — does not redefine — that contract:

- `helianthus-docs-ebus/architecture/ebus_standard/05-execution-safety.md` — execution-safety cadence floor.
- `helianthus-docs-ebus/architecture/ebus_standard/06-nm-adoption.md` — NM adoption / stream invariants.

Concrete obligation: the gateway NM runtime MUST NOT emit an unsolicited 07 FF at a rate that violates the arbitration floor documented in the two chapters above. Observed via `internal/nm_runtime/nm_runtime_test.go` cadence assertions. Any regression in those tests is a rollback trigger for the gateway (see §6).

---

## §6 Rollback criteria per repo

Each subsection states (a) the concrete signals that would trigger rollback, (b) the rollback mechanism. "Revert merge SHA X" refers to the squash-merge SHA of the predecessor PR, recoverable via `gh pr view` on the linked PR.

### §6.1 helianthus-docs-ebus

- **Rollback triggers:**
  - A consumer (M5_PORTAL or M5b HA) reports that a documented forward-compat rule (M4D ch.13 §4) contradicts observed producer emission.
  - A Vaillant 0xB5 behavior documented in `architecture/ebus_standard/*` is refuted by a live-bus transcript from the BASV2 rig.
- **Mechanism:** revert the M4D doc lock PR (ch.13) via `gh pr revert` on the squash-merge SHA; the chapter returns to an earlier state the code no longer references. Code side (gateway + consumers) continues to emit minor=1 envelopes; only the normative prose rolls back.

### §6.2 helianthus-ebusgo

- **Rollback triggers:**
  - TinyGo build breakage on embedded responder harness.
  - Allocation-budget regression on the 0xB5 hot path detected via the ebusgo bench harness.
  - `timing_harness.go::responderAckBudget` found empirically too tight or too loose once BASV2 measurement lands (see §7).
- **Mechanism:** revert the merge SHA of the responder harness PR in ebusgo; gateway continues to compose minor=1 envelopes but the responder capability provider falls back to `scope=none` (fail-closed by construction — the `SetResponderCapabilityProvider` DI path supports nil, and `cmd/gateway/main.go` emits no capability key when the runtime transport is non-enumerated).

### §6.3 helianthus-ebusreg

- **Rollback triggers:**
  - Breaking change to an exported type in the registered-catalog surface, detected by `golden_catalog_test.go` in gateway.
  - B524 dual-namespace opcode decoding drift for GG ∈ {0x08, 0x09, 0x0A, 0xEC}.
- **Mechanism:** revert the merge SHA of the offending ebusreg PR; gateway pin-bump in `go.mod` reverted by a follow-up PR. Goldens in `helianthus-ebusgateway/mcp/ebus_standard/testdata/` remain stable because they are ebusreg-version-independent at v1.1.

### §6.4 helianthus-ebusgateway

- **Rollback triggers:**
  - Any FC-1..FC-7 row in §2 going from PASS to FAIL.
  - Any CM-01..CM-12 row in §2 going from PASS to FAIL.
  - NM 07 FF cadence floor violation observed in `internal/nm_runtime/nm_runtime_test.go`.
  - Responder capability provider returning a capability for a non-enumerated transport (regression of `cmd/gateway/wiring_test.go` nil-on-non-enumerated guarantee).
- **Mechanism:**
  - For M4c2 regression: revert `547fd4e feat(M4c2): gateway responder runtime for FF 03-06 + meta.capabilities.responder emission` — the envelope composer reverts to minor=0 (no capability key); consumers fail-closed per rule 1.
  - For M5_PORTAL regression: revert `205c2a8 feat(M5_PORTAL): ebus_standard L7 consumer UI + XSS-hardened decode sandbox`; consumer surface rolls back independently of envelope contract.
  - Feature-flag disable: set `TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` together with a non-empty `TRANSPORT_GATE_OWNER_REASON` per AGENTS.md §Gates and validation, and disable responder emission at the bootstrap site by calling `SetResponderCapabilityProvider(nil)` unconditionally in `cmd/gateway/main.go` while a code-level revert is prepared.

### §6.5 helianthus-ha-integration

- **Rollback triggers:**
  - M5b no-op compat shim incorrectly surfaces the responder signal as a user-visible HA entity (violation of M5b no-op contract).
  - HA integration attempts responder invocation on an ENH or ebusd-tcp transport pair where the gateway emits `active.scope != full` (should be a no-op at minor=1 for HA).
- **Mechanism:** revert the M5b HA merge SHA; HA consumer ignores `meta.capabilities.responder` as it did pre-M4c2. Gateway-side envelope continues to emit the key; HA stops reading it.

---

## §7 Timing budget: BENCH-REPLACE carry-forward

**Origin.** `helianthus-execution-plans/ebus-standard-l7-services-w16-26.implementing/decisions/m4b2-responder-go-no-go.md` §7.1(1): the responder ACK timing budget was approved as a literature-backed placeholder to unblock M4c2. The decision doc requires a follow-up measurement once a BASV2 live-bus rig is available.

**Current placeholder.** `15 * time.Millisecond` — derived from the eBUS target-response-window published in Vaillant adapter documentation (literature-grade, NOT empirical).

**Production sites of the placeholder.**

1. `helianthus-ebusgo/protocol/responder/timing_harness.go` — the canonical site; exported as `responderAckBudget` (or equivalent symbol). This is the authoritative constant.
2. `helianthus-ebusgateway/cmd/gateway/main.go` / `internal/nm_runtime/responder_runtime.go` — any gateway-side reflection of the budget used to decide cadence floor in responder emission. Grep for `15 * time.Millisecond` or direct import of `responderAckBudget` from ebusgo.

**Operator follow-up obligation (carry-forward).** The operator MUST, in a separate manual PR:

1. Attach a BASV2 rig to the passive tap.
2. Run the ebusgo responder harness bench (instructions in `helianthus-ebusgo/protocol/responder/`) and record the p99 ACK window over ≥10k frames under representative bus load.
3. Commit the measured value as the new budget (supersedes the 15ms placeholder) in `helianthus-ebusgo/protocol/responder/timing_harness.go`. Bump any gateway-side pin via `go.mod` in a follow-up PR.
4. Flip this matrix artifact's §7 status line from `PLACEHOLDER` to `MEASURED (p99 = <value> ms over <N> frames)` with the measurement date + rig identifier.

**Status:** `PLACEHOLDER` — no measurement attempted in this session (no BASV2 rig available).

**M6a DOES NOT attempt the measurement.** This obligation is intentionally documented and deferred, not actioned. No change to the 15ms value lands via this PR.

---

## §8 Sign-off conditions

**M6a is MERGED when** the matrix artifact (this file) and the rule-7 forward-compat golden + tests land via this PR.

**M6a is CLOSED (issue #513 closed out completely)** when the BENCH-REPLACE §7 carry-forward obligation is fulfilled by the operator:

1. BASV2 measurement committed to ebusgo.
2. This file's §7 status flipped from `PLACEHOLDER` to `MEASURED`.
3. Cross-repo pin bumps done.

Until step (3), the cruise-control FSM holds M6a in `hold-for-operator` state. The PR label `hold-for-operator` signals this to the merge-gate skill.
