# Gateway #965 Tesla Gen3 HSC retained owner author report

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/965
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/969
- Branch: `issue/965-tesla-hsc-retained-owner`
- Base: `c139d0e6ae59b4f925ac02a7cadf09db53278e13`
- Implementation HEAD: `0d1bb2f9b9a8723f9c7b189df20746ac6e1e2f4e`
- Implementation tree: `8097ffb2d42fb158d191d9b4df0c8e02e12cdf71`
- Blocking-findings remediation HEAD:
  `c5bf9bcbfe61d627393e190a0e04a1be1be4d8eb`
- Blocking-findings remediation tree:
  `324d4ee08a6be0c42418311470bfb7ab08e9de61`
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

The fresh independent review of PR #969 at `7874714491e9cf533779b3679212316df54a400e`
reported three blocking findings. Remediation commit
`c5bf9bcbfe61d627393e190a0e04a1be1be4d8eb` corrects all three:

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

This PR does not modify adaptermux. The deterministic correction belongs to
open issue #968, which should observe provider return from inside the fake
transport or another event within the protected call. The remediation's single
complete local CI run passed the same full adaptermux race suite; no hosted
retry loop was used.

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
- Review: the three validated blockers from the earlier independent review are
  corrected. A fresh independent exact-HEAD review remains required before
  merge; the author did not review the remediation.
- Merge: not performed. The implementation is not present on remote `main`.
- Issue: remains open. This PR uses `Refs #965`.

## Runtime routing note

The assignment requested `gpt-5.6-sol` at high effort. The applied task runtime
identified itself as GPT-6, and its effort setting was not exposed. The author
did not silently substitute another task or claim the requested routing was
applied.

The PR branch is the durable source for this report; `/tmp` log paths identify
the author-side files whose hashes are recorded above.
