# Gateway #965 Tesla Gen3 HSC retained owner author report

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/965
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/969
- Branch: `issue/965-tesla-hsc-retained-owner`
- Accepted base: `138eea47e75b99e008beb24c7ab5938f02690845`
- Rebased implementation HEAD: `366023a6c717a2ead3521dad544c1e45122895d9`
- Rebased implementation tree: `739845b260783ff8b7564faa81ff31d844f82c49`
- Rebased blocking-findings remediation HEAD:
  `afa79f777d1f9d1a2fc6bbc2e31bf67bd62ef8ef`
- Rebased blocking-findings remediation tree:
  `1dd518ff457a1d4a83051bd972c1247524f8accd`
- Rebased validated source HEAD:
  `b824e43241c726d7867e69bf25ddb786c266f9cf`
- Rebased validated source tree:
  `5068bd638b4b7204038c961b6faee7b73019e385`
- Optional-registration and record-lifecycle remediation HEAD:
  `5626d8352d9b495f7c71477d96db67f24a87e6db`
- Optional-registration and record-lifecycle remediation tree:
  `6aabe69395e4ec183abeed2092a6d9ccb02d55b6`
- Registry dependency: `helianthus-modbusreg`
  `v0.6.8-0.20260905063817-ed75fdfbed0d`

This is the bounded, non-send product slice accepted for issue #965. It does
not construct requests, open serial endpoints, exchange frames, activate a
profile, hold credentials, authorize operations, write a device, or acquire
live records. It does not close #965.

## Implemented behavior

The disabled-by-default Tesla Gen3 HSC retained owner accepts completed
outcomes for the exact `wc3_24_44_3` registry profile. Configuration requires
stable endpoint, asset, source, source epoch, clock epoch, EVSE, connector,
driver generation, and unicast node identities. Disabled configuration with
active fields and incomplete or conflicting enabled identities fail closed.

Persistent t7/t8 outcomes and provisional t25/t26 plus t27/t28 outcomes are
ingested through distinct typed records. Each completed exchange must provide:

- the configured source, epoch, generation, node, operation, and operation
  version;
- a strictly increasing correlation ID and non-regressing receipt axes;
- a successful terminal outcome;
- retained request and response payloads plus their exact RTU ADUs.

The owner reconstructs and compares request ADUs, decodes each response ADU
against the request, validates the completed operation sequence in
`helianthus-modbusreg`, and passes the typed values and exact bodies through
the registry constructors. Malformed bodies, mixed identities, sequence
mismatches, duplicate or late outcomes, and forged typed values are rejected
before retained state changes. A rejected sibling leaves the last accepted
persistent and provisional records unchanged.

The store is bounded to the latest persistent exchange and latest provisional
set/readback pair. Native evidence returned to callers is cloned, including
payload and ADU slices. The existing Tesla EVSE SemReg publication consumes
the retained typed records, and the gateway composes its detached native and
semantic reads into the accepted MCP methods and authenticated M2M GraphQL
`SemanticEVSECurrent` field. These read paths perform no ingestion or I/O.

`Fence` withdraws all native and semantic reads before successor admission.
Only the exact next driver generation with a new source epoch and unchanged
stable identity can start a successor. Shutdown also withdraws reads.

The production boundary script now permits this owner-backed read composition
while rejecting direct semantic construction or ingestion in lifecycle code.
It separately rejects serial opening, exchange, request construction, device
write calls, and activation markers in the retained owner.

## Independent-review remediation

The fresh independent review of PR #969 at pre-rebase head
`7874714491e9cf533779b3679212316df54a400e` reported three blocking findings.
Rebased remediation commit `afa79f777d1f9d1a2fc6bbc2e31bf67bd62ef8ef`
preserves the corrections for all three:

- A persistent-only update keeps the provisional native record and its original
  evidence, but omits that unchanged sibling from the combined semantic publish.
  This withdraws allocated current instead of assigning it the persistent
  outcome's later receipt. The regression places the persistent update after
  the provisional timeout and verifies configured current remains exact while
  allocated current is withheld through the MCP provider and an M2M GraphQL
  handler carrying an authenticated principal.
- Both the Growatt-only and composite providers implement one Portal storage
  availability capability. A started Growatt runtime remains available when
  Tesla is composed, while Tesla-only and failed-Growatt compositions remain
  unavailable.
- `Successor` reserves the fenced transition while holding the owner lock.
  Successful construction consumes it exactly once; construction failure
  releases one retry. Repeated and 32-way concurrent admission regressions prove
  that only one active owner can use the next epoch and generation.

The three review threads were replied to with their correction and test evidence
and deliberately left unresolved for the next independent review.

## RED-first and validation evidence

The first focused owner test run was intentionally RED:

```text
GOWORK=off go test ./cmd/gateway -run '^TestTeslaHSCRetainedOwner' -count=1
```

It failed to compile because `TeslaGen3HSCRetainedConfig`,
`TeslaGen3CompletedExchange`, and `startTeslaHSCRetainedOwner` did not exist.
The tests therefore preceded the production types and implementation.

The final focused race run passed:

```text
GOWORK=off go test -race ./cmd/gateway -run '^TestTeslaHSCRetained' -count=1
ok github.com/Project-Helianthus/helianthus-ebusgateway/cmd/gateway 12.552s
```

Log: `/tmp/gateway965-focused-race.log`  
SHA-256: `849c823a3a0cfda10d35272cc371e23bd293023f47522a8abcc99b3869fcd8a5`

The complete repository CI passed:

```text
GOWORK=off ./scripts/ci_local.sh
```

It passed formatting, Portal assets and tests, `go vet`, all builds including
Linux 386 and ARM variants, the full Go race suite, source-selection schema
coverage, 216 Python tests, `golangci-lint` with zero issues, Modbus transport
conformance, Growatt and Tesla SemReg mapping gates, and passive-smoke
classification. The passive smoke gate was not triggered because this change
contains no live or runtime transport acquisition.

Log: `/tmp/gateway965-ci-pass.log`  
SHA-256: `0a21d4915493a9aeca080acf10b8c0dbd978bcbb346df4c60f5ae986e800854e`

The standalone affected transport gate also passed:

```text
GOWORK=off ./scripts/transport_gate.sh
transport gate: PASS (Modbus RTU production composition and pinned endpoint conformance).
```

Log: `/tmp/gateway965-transport-final.log`  
SHA-256: `f8580a2573ebcb3dada0fd8f55e3d8b009dbfa061ee51690727f14e4a01e2572`

The remediation focused race run passed:

```text
GOWORK=off go test -race ./cmd/gateway -run 'Test(TeslaHSCRetained|GrowattStoragePortalAvailability)' -count=1
ok github.com/Project-Helianthus/helianthus-ebusgateway/cmd/gateway 12.687s
```

Log: `/tmp/gateway969-remediation-focused-race.log`
SHA-256: `85a63c0f38046e1c0c6e125f04b496f2256ce1a46e95eab81156254b246b8931`

The complete repository CI also passed after remediation, including the full
Go race suite, all 216 Python tests, zero lint findings, transport conformance,
both SemReg mapping gates, and passive-smoke classification:

Log: `/tmp/gateway969-remediation-ci.log`
SHA-256: `a4a986163086ccbece0f685d484eb4107b08e014296efe2c6f8a04cac56ea200`

The standalone affected transport gate passed after remediation:

Log: `/tmp/gateway969-remediation-transport.log`
SHA-256: `9a30ab58105b974e720096cf42f47338abf11a9a7f9f64081dbd6596c1d9bd5f`

## Accepted-main rebase validation

PR #969 was rebased onto accepted `main`
`138eea47e75b99e008beb24c7ab5938f02690845`. The validated rebased source was
`b824e43241c726d7867e69bf25ddb786c266f9cf`, tree
`5068bd638b4b7204038c961b6faee7b73019e385`.

The rebased focused retained-owner race run passed:

```text
GOWORK=off go test -race ./cmd/gateway -run 'Test(TeslaHSCRetained|GrowattStoragePortalAvailability)' -count=1
```

SHA-256: `e839d8ba1dd09c59e48d44f9893750236fed5acf18ba2ec769974301f0b5c717`

The complete repository CI passed on the rebased source, including the full Go
race suite, all transport and mapping gates, lint, builds, Python checks, and
passive-smoke classification.

SHA-256: `5a70eaa553e5f72fa12f887a31ed269e6b96d5024572f1fd7a777924bdf063df`

## Later feedback remediation

The complete feedback inventory after the accepted-main rebase contained four
additional valid blockers. Commit
`5626d8352d9b495f7c71477d96db67f24a87e6db`, tree
`6aabe69395e4ec183abeed2092a6d9ccb02d55b6`, corrects all four:

- Tesla retained native registration is independent of Modbus TCP core
  availability. A Tesla-only provider registers
  `modbus.v1.tesla.gen3.evse.current_limit.get` and
  `semantic.v1.evse.current.get` while TCP raw/profile/PV and unrelated Tesla
  tools remain absent.
- Optional owner registration consults the composite provider's actual
  capability. Tesla-only composition no longer advertises either Growatt tool;
  Tesla-plus-Growatt composition retains both Growatt native and semantic tools.
- Disabled Modbus TCP endpoint normalization preserves the independent Tesla
  retained configuration, just as it preserves the independent Growatt RTU
  configuration.
- Semantic evidence now carries validated per-record receipt coordinates. A
  newer provisional update uses the retained persistent outcome's original wall
  and monotonic receipt, so it cannot refresh configured-current freshness. The
  earlier persistent-only withdrawal behavior remains unchanged.

The deterministic Tesla-only registration regression calls both advertised
read-only tools and proves the TCP core and absent Growatt tools remain
unregistered. The composite case retains both Growatt tools. Separate tests
cover disabled-TCP normalization and both sibling lifecycle directions.

Post-correction focused race:

```text
GOWORK=off go test -race ./cmd/gateway ./mcp -run 'Test(ResolveModbusEndpointFileDisabledPreservesIndependentTesla|TeslaHSCRetained|TeslaGen3EVSE|GrowattStoragePortalAvailability|GrowattBMSRS485V202.*Registration|GrowattBMSRS485V202Coreless)' -count=1
```

SHA-256: `1d6c1f3da33e07555b2d78d0e104f8280476495051dbaae9bc933a7158a90837`

The complete local CI passed on the corrected source, including the full Go
race suite, 216 Python tests, lint, all builds, transport conformance, both
SemReg mappings, and passive-smoke classification.

SHA-256: `3b3f17825d73e96c9cc5238d57d22ea6116e08b007f012b8e0432b62e7f522aa`

Affected transport gate SHA-256:
`e9e9162087d0761e8b09864d874aba637efa6c1e727daed1a4873373ac01b3ce`

Tesla SemReg mapping gate SHA-256:
`c65326e84d5bc5fa42c712b1752e75ad87ab1b00bbda87b63ef8c10cf5a4e079`

All four later feedback threads were replied to with their correction and test
evidence and deliberately left unresolved for fresh exact-HEAD review.

## Hosted adaptermux failure diagnosis

Hosted run `34512623473`, test job `102990233209`, failed only
`TestManagedConnectionLossLinearizesProxyAdmissionAndProviderUse/blocked_write_drains_before_BACKOFF_publication`
at `internal/adaptermux/connection_health_test.go:231` with
`connection-loss callback crossed an admitted Write`.

Production `reconnect` first fences new work, closes the transport, then takes
`connectionUseMu` for writing. The admitted `doSend` holds the read lease until
the transport `Write` returns and its deferred lease release runs. The test's
`writeDone` channel is closed by the caller goroutine only after `doSend`
returns, outside that protected lease. After the read lease releases, the Go
scheduler may run the waiting reconnect goroutine and its callback before it
runs the caller's next `close(writeDone)` statement. The hosted assertion
therefore observes an unprotected caller-side scheduling order, rather than a
provider call crossing the production fence.

This PR does not modify adaptermux. Accepted base
`138eea47e75b99e008beb24c7ab5938f02690845` contains the deterministic test
correction merged through PR #970. The complete CI on the rebased source passed
the full adaptermux race suite and all remaining gates; no hosted retry loop was
used.

Hosted failure log: `/tmp/gateway969-hosted-test-failure.log`
SHA-256: `607c4eae1b8b400ec3ae2c9c18b6128f28985a31d8cfaab57f85254947a34871`

## Gate and handoff state

- Documentation gate: satisfied by
  `docs/tesla-gen3-hsc-retained-owner.md` and its README link.
- Transport gate: passed with the pinned Modbus dependency and affected owner
  tests.
- SemReg gate: passed for the existing Tesla EVSE mapping.
- Smoke gate: not triggered; no live acquisition or physical test was
  performed or claimed.
- Review: the three earlier and four later validated blockers are corrected. A
  fresh independent exact-HEAD review remains required before merge; the author
  did not review the remediation.
- Merge: not performed. The implementation is not present on remote `main`.
- Issue: remains open. This PR uses `Refs #965`.

## Runtime routing note

The assignment requested `gpt-5.6-sol` at high effort. The applied task runtime
identified itself as GPT-6, and its effort setting was not exposed. The author
did not silently substitute another task or claim the requested routing was
applied.

The PR branch is the durable source for this report; `/tmp` log paths identify
the author-side files whose hashes are recorded above.
