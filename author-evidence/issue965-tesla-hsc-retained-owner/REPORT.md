# Gateway #965 Tesla Gen3 HSC retained owner author report

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/965
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/969
- Branch: `issue/965-tesla-hsc-retained-owner`
- Base: `c139d0e6ae59b4f925ac02a7cadf09db53278e13`
- Implementation HEAD: `0d1bb2f9b9a8723f9c7b189df20746ac6e1e2f4e`
- Implementation tree: `8097ffb2d42fb158d191d9b4df0c8e02e12cdf71`
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

## Gate and handoff state

- Documentation gate: satisfied by
  `docs/tesla-gen3-hsc-retained-owner.md` and its README link.
- Transport gate: passed with the pinned Modbus dependency and affected owner
  tests.
- SemReg gate: passed for the existing Tesla EVSE mapping.
- Smoke gate: not triggered; no live acquisition or physical test was
  performed or claimed.
- Review: not performed by the author. A fresh independent exact-HEAD review
  remains required before merge.
- Merge: not performed. The implementation is not present on remote `main`.
- Issue: remains open. This PR uses `Refs #965`.

## Runtime routing note

The assignment requested `gpt-5.6-sol` at high effort. The applied task runtime
identified itself as GPT-6, and its effort setting was not exposed. The author
did not silently substitute another task or claim the requested routing was
applied.
