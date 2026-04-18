# Helianthus v0.5.0 — Changelog

## Summary

This release is a comprehensive correctness and reliability overhaul driven by a multi-agent audit (407+ findings across 8 products, 60 AI agents, 7 audit rounds). Six of eight products received full remediation, fixing critical protocol bugs (ENH double-escape, SYN-before-first-echo race across 4 codebases, arbitration deadlocks), hardening concurrency/session management, and aligning cross-product invariants. Three production bugs were root-caused and fixed (startup scan timeouts, echo mismatch storms, SYN terminator delivery).

## Breaking Changes

None. All CLI flags, config schemas, and API surfaces remain unchanged. The HA addon `config.json` and `run` script require no updates.

## Gateway (helianthus-ebusgateway)

### Fixed
- AM-NEW-41: SYN terminator consumed instead of delivered to activeCh — root cause of `ok=0 timeouts=46` at startup scan
- AM-NEW-42: Pre-echo SYN race causing 13,904 echo_mismatch events per soak run
- AM-NEW-43: Busy-bus gate on `bytesDeliveredToActive` — SYN terminator gated on actual byte delivery
- AM55: SYN-cancel spurious FAILED events
- AM56: STARTED-mismatch potential deadlock
- AM57: Premature ownership release during active transactions
- 54 audit findings (AM1-AM58) addressed in PR #501
- PR #502: cancel-to-reconnect flow, atomic active-path recheck, shutdown gate serialization
- Bounded SYN-path diagnostic ring buffer and txnID scan correlation
- Scoped startup-scan classifier per gateway instance

### Added
- Session hard ceiling (maxSessions=1000, hardcoded) with rejection logging (AM50)
- Echo tracker for detecting and logging echo mismatches
- E2E byte-flow test for adapter-direct path validation

### Changed
- Wire phase tracking expanded with additional state assertions and tests (+187 test lines)
- Passive transport hardened with SYN delivery and shutdown ordering fixes

## ebusgo Library

### Fixed
- Post-grant pre-echo SYN suppression — idle SYN between arbitration grant and first echo now filtered (cross-product bug family)
- Post-grant SYN filtering applied in RequestInfo RECEIVED path
- RequestInfo defer ordering — cleanup runs before writeMu.Unlock
- RequestStart STARTED-before-window race — set awaitingStart BEFORE Write
- awaitingStart deadlock fixes across RequestInfo, parse errors, and window deadline
- Async path closes awaitingStart only on matching STARTED initiator
- Bounded post-grant pre-echo window with deadline to prevent stale state

### Added
- 146 new lines of bus protocol tests
- 177 new lines of ENH transport tests

### Changed
- RequestInfo refactored to use explicit cleanup at every exit instead of defers
- awaitingStart uses strict control-event cap

## Proxy (helianthus-ebus-adapter-proxy)

### Fixed
- PX-SYN-RACE: Suppress idle SYN to owner between grant and first SEND (cross-product bug family)
- PX60: Concurrent INFO theft — re-check INFO ownership after WriteFrame
- Wall-clock silence threshold for blackhole detection (replaces monotonic-only check)
- Serialized all upstream writes to prevent interleaving, fixed lock ordering
- INFO sequence capture under pendingInfoMu before unlock
- 7 runtime liveness fixes + 5 test quality fixes (owner R10 review)
- XR semantics alignment, SYN guard, INFO overlap prevention, wirelog rotation

### Added
- Complete XR cross-repo invariant test suite (12/12 tests)
- Config fields for `MaxConcurrentSessions`, `AcceptRateLimit`, `WireLogMaxSize`, `SourceAddressPolicy` (programmatic only, not CLI-exposed)
- Config validation: rejects empty ListenAddr/UpstreamAddr and invalid SourceAddressPolicy values
- Broadcast test suite (+446 lines)

### Changed
- Server internals expanded significantly (+581 lines, -17 lines across server.go and tests)
- Operations runbook updated with new config field documentation

## VRC Explorer

### Fixed
- XR-SYN-GUARD: Suppress post-grant idle SYN before first echo (cross-product bug family)
- VE1-VE33: Full audit remediation (27 fixed, 5 verified correct, 1 respins, 1 N/A)
- VE-NEW-01: CRC=0xAA false timeout — SYN guards in ENH response path
- VE-NEW-07: Abort on 3x STARTED with wrong initiator address
- Mismatch counter reset on RECEIVED traffic in arbitration
- Mismatch counter alignment + B555 half-sentinel validation
- Surface unknown ENH commands to callers instead of dropping

### Added
- R6 LOW+INFO findings addressed — zero deferrals

## Protocol Documentation (helianthus-docs-ebus)

### Fixed
- DOC-NEW-04: CRC scope ambiguity — root cause of escape handling bugs across 3 codebases
- DOC14: False claim that ENS=ENH
- DOC2: SYN handling documentation error in ebus-overview.md
- Factually incorrect ENH wire-escape decoding claim removed
- Contradictory SYN-abstraction note removed
- INFO interleaving + UDP fallback lease start documentation corrected
- Unknown-cmd encoding + INFO overlap contradiction fixed
- 11 findings from 5-round adversarial pass on spec + matrix

### Added
- Conformance catalog moved to architecture/ directory
- ENH protocol spec fully reviewed and corrected across 10+ commits

## Wireshark Plugin

### Fixed
- WS12: Terminology-gate CI compatibility (split legacy needles)
- WS13: ENS command direction dissection
- WS15/WS20/WS26: Expanded fixture set covering remaining dissection paths
- Lua 5.2 compatibility in bitwise helper resolution
- Truncated eBUS frames handling, reply label, parser messages

### Added
- WS25: Seven missing Vaillant B5xx opcode labels
- WS19: Dissector integration test suite with tshark-driven validation and PCAP fixtures
- Public-API contract hardening (WS13/WS18)

## HA Addon

### Changed
- No changes required. The addon `config.json` schema and `run` script are fully compatible with all gateway and proxy changes in this release. All new proxy config fields (`MaxConcurrentSessions`, `AcceptRateLimit`, `WireLogMaxSize`, `SourceAddressPolicy`) are programmatic-only and use safe defaults (unlimited/disabled) when not set.

## Known Open Items

- **tinyebus**: 33 findings (TE1-TE33) — NOT STARTED (pre-hardware scaffold)
- **PIC firmware**: 70 findings (PF1-PF70) — NOT STARTED (pre-hardware scaffold, 6 CRITICAL)
- Version bump in addon `config.json` from `0.4.0` to `0.5.0` still needed before release
