# Gateway #965 Tesla Gen3 HSC retained owner author report

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/965
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/969
- Branch: `issue/965-tesla-hsc-retained-owner`
- Accepted base: `2daae4ca0318d014fe7e88aea76a2c54c1f76721`
- Rebased implementation HEAD: `c3dc4cc36095a5dce101f76d163c7d20996c355a`
- Rebased implementation tree: `91c8b71923dcdf948bcc53670eb58d67f0c80444`
- Rebased blocking-findings remediation HEAD:
  `cf43b8bec5ba34ac62928274f999e315e00a4d1a`
- Rebased blocking-findings remediation tree:
  `ac792831eb02460735dfcedc0a3623fd8c4f3370`
- Optional-registration and record-lifecycle remediation HEAD:
  `615afb748026d0a8dbd03a32d2406fe9dc72d7f4`
- Optional-registration and record-lifecycle remediation tree:
  `920b43bbe091fc7aa1b4e73beabcce73c09205a6`
- Rebased validated source HEAD:
  `ee0f9f682bcec8f2cac5d606e6fc660a2503abb2`
- Rebased validated source tree:
  `a127edf0c906703339110a7a349486cc79f0b88f`
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

PR #969 was first rebased onto accepted `main`
`138eea47e75b99e008beb24c7ab5938f02690845`. The validated source for that
historical rebase was `b824e43241c726d7867e69bf25ddb786c266f9cf`,
tree `5068bd638b4b7204038c961b6faee7b73019e385`.

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

The four feedback threads available during that correction were replied to with
their correction and test evidence and deliberately left unresolved for fresh
exact-HEAD review.

## Accepted EVSE metrics dependency rebase

Gateway #966 / PR #967 was accepted on `main` as
`2daae4ca0318d014fe7e88aea76a2c54c1f76721`. PR #969 was rebased onto that
exact commit. The one conflict in `cmd/gateway/gateway_cli.go` preserved both
the accepted Prometheus EVSE flag and the retained-owner configuration flags.

The retained owner now implements the accepted narrow
`SemanticEVSEPrometheusProvider` read seam and production composition supplies
that owner to the passive metrics callback. A scrape can only reevaluate the
already accepted detached SemReg tuple; it has no acquisition, publication,
request, send, or control authority. The owner returns unavailable after its
generation is fenced. A deterministic Tesla-only regression proves the EVSE
domain is populated from retained state while unrelated PV and storage domains
remain absent, and the concurrent retained-read regression now includes the
Prometheus seam.

The accepted dependency also requires a publication instant at or after the
evidence instant. The offline retained-owner fixture therefore uses a stable
past epoch while preserving its exact wall/monotonic ordering assertions.

The final feedback inventory contained one additional valid P2. When a
provisional outcome arrived before the first persistent outcome, it was retained
but omitted from the initial semantic batch. The initial batch now includes that
provisional record with its original receipt axes; later persistent-only updates
still omit an already published unchanged provisional sibling. The regression
first failed with allocated current withheld and then passed after the narrow
condition changed. RED log SHA-256:
`d1521402475741d9ebc818e8e0ce0b53c53170d3ef025bdcebc02ea9d66df607`.

The Prometheus composition regression was intentionally RED before the owner
implemented the accepted interface:

```text
GOWORK=off go test ./cmd/gateway -run TestTeslaHSCRetainedOwnerFeedsDetachedPrometheusEVSEAndFencesLifecycle -count=1
```

It failed to compile because `*teslaHSCRetainedOwner` lacked
`SemanticEVSECurrentAt`. Log SHA-256:
`d2fd7df0aeef356a91d1d3dce49f9d1491fcab929c7a888c10b94c5daccac998`.

The rebased focused race run passed:

```text
GOWORK=off go test -race ./cmd/gateway ./mcp -run 'Test(ResolveModbusEndpointFileDisabledPreservesIndependentTesla|TeslaHSCRetained|TeslaGen3EVSE.*Prometheus|SemanticPrometheus|BindFlagsPrometheusEVSE|GrowattStoragePortalAvailability|GrowattBMSRS485V202.*Registration|GrowattBMSRS485V202Coreless)' -count=1
```

Log: `/tmp/gateway969-rebase-prometheus-focused-race.log`
SHA-256: `92f495561025213360a0d97ebe222eb6df3a2f651718ccfa78653522588f1b58`

Complete local CI passed on source
`6a5f245bee72096f268f07a30eff4a88ab760259`, tree
`102ca7cfa6249aa2eab3fce6212c4c89d1b28b58`. It included all Go race tests,
219 Python tests, zero lint findings, builds, transport conformance, both
SemReg mapping gates, and passive-smoke classification.

Log: `/tmp/gateway969-rebase-ci.log`
SHA-256: `2752f1c4d673b4e0f4349eb400b4773bd1bd402c4d74c15ea29ba6c2ce613f11`

Standalone affected gate hashes:

- Modbus RTU transport: `df52e3fe3f24413661f424598c4223d5e451ef3c0ff699dee0c7d3b0cb9ff02a`
- Tesla SemReg mapping: `3054dc341905d349127269f0a85e6a84d98dabecdf9625d16add98865a22ffd3`
- Tesla owner boundary: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

The additional provisional-first feedback thread was replied to with the
correction and final evidence and left unresolved with the other threads.

## Exact-HEAD lifecycle ordering and age remediation

The independent review of `9602321cf0649ef0b4dba692861a566a47f29a58`
reported that two admitted records can share a coarse monotonic coordinate
while the later record has a later wall receipt. The aggregate lifecycle now
uses monotonic order first and the later wall receipt as the tie-break. The
normal regression accepts correlation-1 persistent followed by correlation-2/3
provisional outcomes at the same 10-second monotonic coordinate, publishes both
facts at the later aggregate wall instant, and proves that configured and
allocated current retain their original per-record wall and monotonic receipts.
A lower monotonic coordinate remains a true regression and leaves the records,
evidence, and publication sequence unchanged. The same fixture is covered by
32 concurrent native, semantic, and Prometheus readers under the race detector.

The live PR inventory also contained delayed-publication findings. The
publication already sealed receipt-to-publication elapsed time for detached
Prometheus reads. MCP and GraphQL now use that same immutable same-epoch point
as their read-clock origin and add subsequent elapsed time before applying the
read high-water. A queued allocation therefore keeps only its remaining native
lifetime. Deterministic MCP/authenticated-GraphQL parity regressions preserve
the already-expired case and exercise a 60-second allocation published 30
seconds late: it remains exact at 59 seconds total age, becomes withheld at 60
seconds, and stays withheld afterward. Configured current remains independently
exact while its freshness becomes stale. The read origin retains rollback,
overflow, and clock-epoch failure coverage and has no provider or native I/O
path.

RED evidence:

- Equal monotonic/later wall rejection:
  `eadc1df26a173dfe7c9c4953e72ccbe425a85bc5b37ba177f05485af36235547`
- Delayed MCP/GraphQL allocation incorrectly fresh:
  `6d24919d9ff99ecdf89b7535a7aca649615383871276893c173860f0e606d768`
- Partial publication delay incorrectly failed to accumulate:
  `e0f82b1c65878069ee152c17214f0252b07392c885ef9cea1cc7a4a8b5fac679`

Final focused race command:

```text
GOWORK=off go test -race ./cmd/gateway ./mcp -run 'Test(ResolveModbusEndpointFileDisabledPreservesIndependentTesla|TeslaHSCRetained|TeslaGen3EVSE|SemanticPrometheus|BindFlagsPrometheusEVSE|GrowattStoragePortalAvailability|GrowattBMSRS485V202.*Registration|GrowattBMSRS485V202Coreless)' -count=1
```

Focused race SHA-256:
`35e30727dbd7ef256b99b7cb26131fec6565c1e31528f20c3811d00053fb4afb`

Complete local CI passed on source
`ee0f9f682bcec8f2cac5d606e6fc660a2503abb2`, tree
`a127edf0c906703339110a7a349486cc79f0b88f`, including all Go race tests,
219 Python tests, zero lint findings, all builds, transport conformance, both
SemReg mapping gates, and passive-smoke classification. SHA-256:
`bc6fd165ff444aee180b57f66e43ac9029c8a7b21235584fd35ca932d9c3a638`.

Standalone final gate hashes:

- Modbus RTU transport: `d434b3b65287a18810ccfe58a450cad09d2ead6bb8cbb98d1e7baee873c2c666`
- Tesla SemReg mapping: `e79357ae6d11df624b42e945db859586b1f775fec9f28e17819fab56b3c8aaca`
- Tesla owner boundary: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

Independent report SHA-256:
`fd2410f5c9d44a7ffe6fa3c82d9cd25186a27e7e380e7d424624642e26c267e6`.
The three trailing-space P3 lines were removed during this report refresh.
The tenth partial-delay thread was replied to with the final correction and
evidence and left unresolved for fresh exact-HEAD review.

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
- Review: the ten earlier findings plus the partial-delay accumulation finding
  are corrected. A fresh independent exact-HEAD review remains required before
  merge; the author did not review the remediation.
- Merge: not performed. The implementation is not present on remote `main`.
- Issue: remains open. This PR uses `Refs #965`.

## Runtime routing note

The assignment requested `gpt-5.6-sol` at high effort. Actual provider runtime
metadata was not exposed, so the author does not claim which model or effort
was applied.

The PR branch is the durable source for this report; `/tmp` log paths identify
the author-side files whose hashes are recorded above.
