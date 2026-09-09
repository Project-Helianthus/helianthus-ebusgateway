# Issue #953 — Growatt BMS RS-485 native composition

## Scope

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/953
- Branch: `issue/953-growatt-bms-native-composition`
- Base: `2ad927b6986c6ce01ed55ff1da25b38625188f44`
- RTU dependency: `github.com/Project-Helianthus/helianthus-modbus`
  `v0.3.1-0.20260909115211-f670287d0d86`
  (`f670287d0d864e9d669ddba0d21a217beadc45f9`).

## Delivered composition

The existing Modbus runtime boundary now contains one disabled-by-default,
explicit Growatt BMS RS-485 V2.02 path. It opens the public
`RTUProductionEndpoint` only with complete valid source/lifecycle, unit, serial,
timing, and response-bound configuration. Its selected tuple is fixed to
`1xSxxP ESS` / `Rev2.01` / `V2.0` / `2.02`.

The endpoint owns serial lifecycle, FC03 framing/correlation, immutable ADU
receipts, recovery, and transport generation. Gateway admission permits only
the four ordered unicast FC03 slices `(0x0001,7)`, `(0x000d,29)`, `(0x0100,12)`,
and `(0x010d,2)`. The existing runtime and `helianthus-modbusreg` observer own
decoding and qualification. No serial opener, decoder, correlator, retry loop,
or evidence owner was duplicated.

Each complete qualified sample commits an immutable native envelope with
owner-assigned observation ID/revision; explicit source ID/epoch and driver
generation; receipt wall/monotonic and clock coordinates; transport generation;
and all four request/response ADUs and words. Partial reads, mismatched
responses, stale/mixed generations, clock changes, disabled/invalid input, or
qualification failure return no status and commit no envelope.
`outbound_allowed` is always false.

Upstream terminal `write_fault` and `transport_fault` mark recovery required;
the failed sample is never retried. Before only a later poll, the serialized
provider invokes the upstream bounded `Recover`, then starts a fresh four-read
sample at its successor transport generation. Exception/admission/qualification
failure does not request recovery; failed recovery stays unavailable.

Without Modbus TCP, the RTU path registers only
`modbus.v1.growatt.bms.rs485.status.get`; it adds no TCP raw/profile, Tesla, or
SemReg PV tool. Portal raw Modbus remains a TCP-core diagnostic surface: an
enabled raw-read setting still leaves its capability false and route unavailable
for disabled and BMS-only composition, while TCP-only and TCP+BMS retain it.
No SemReg, GraphQL, Home Assistant, Matter, eeBUS, Prometheus, or control
surface was added.

## Validation

Focused normal tests passed:

```text
GOWORK=off go test ./cmd/gateway ./mcp \
  -run 'Test(GrowattBMSRS485|PortalRawModbusUsesOnlyTCPAvailableComposition)' -count=1
PASS
```

Focused race tests passed:

```text
GOWORK=off go test -race ./cmd/gateway ./mcp \
  -run 'Test(GrowattBMSRS485|PortalRawModbusUsesOnlyTCPAvailableComposition)' -count=1
PASS
```

They cover exact I/O count/order and unit binding, immutable evidence readback,
disabled/partial/invalid-unit/stale-generation rejection, response binding,
source epoch/driver/transport generation replacement, and native-only
registration.

The correction additionally proves a failed partial fault commits no envelope;
the next call performs exactly one recovery and exactly four fresh FC03 reads;
failed recovery fails closed; and concurrent polls and close serialize through
the provider lifecycle. The optional-provider matrix proves fully disabled,
TCP-only, BMS-only, and TCP+BMS paths expose Growatt only when a runtime exists.
They also assert the exact registration/invocation matrix: disabled registers no
Modbus tool; TCP-only core tools only; BMS-only Growatt only; TCP+BMS both.
The lifecycle-shaped test supplies an explicit disabled concrete runtime pointer
to the factory for both fully-disabled and TCP-only paths, proving no typed-nil
interface can register Growatt. It applies that same factory matrix to Portal:
disabled and BMS-only advertise no raw Modbus capability and return `404` for
the raw route; TCP-only and TCP+BMS advertise it and route into the existing
request validation.

The repository transport gate classifies this RTU production-composition path
separately from eBUS M6a. Its explicit non-test trigger set covers RTU config,
CLI binding, runtime, lifecycle wiring, endpoint-file normalization, gateway
provider composition, MCP registration, and Portal provider binding. A diff
that changes the exact direct `helianthus-modbus` selection in `go.mod` also
triggers; unrelated module bumps do not. It runs the gateway's deterministic
composition fixtures, then inventories every pinned endpoint test before
running the anchored `^TestRTUProduction` suite. The required f670 inventory
is `ReadRetainsImmutableCorrelatedEvidence`, `ExceptionDoesNotFenceButShortWriteDoes`,
`RejectsUnadmittedReadBeforeWrite`, `RecoveryWaitsForRetiringReadOwnership`,
`FourSequentialReadsRemainBounded`, `CancellationFencesAndPartialFramesRetainEvidence`,
`MalformedAndCRCFramesRemainTerminalEvidence`,
`RejectsTimingAndRecoveryBoundMismatch`,
`RecoveryDiscardsDelayedOldGenerationFrame`, and
`RejectsRecoveryBoundsAndNoByteTimeout`. Missing or duplicate expected names,
gateway failures, and endpoint conformance failures fail closed. The gate tests
prove every composition input triggers, every such trigger fails closed for a
failed command, and a partial endpoint inventory is rejected. T01..T88 remains
the required gate for eBUS transport/topology changes.

Final configured CI passed:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

It covers `gofmt`, Portal Node `93/93`, assets, vet, native/Linux builds, full
`go test -race ./...`, source-selection schema coverage, Python suites (`168`,
`6`, `15`, `8`, `6`, `2`), `golangci-lint` (`0 issues`), the Modbus RTU
composition/pinned-endpoint transport gate, and the passive smoke gate. The
Modbus RTU gate passed; passive smoke was not triggered. Exact final CI log
SHA-256: `f0053d83c5f0f6a8ef11e882841a3e2599854ee46f0ca5528ba8a12affdfc0ca`.

## Boundary

This is offline composition evidence, not physical qualification, device
compatibility, serial timing proof, deployment, or hardware operation. A future
authorized hardware qualification and separate SemReg cutover remain required.
No credentials, device I/O, deployment, service action, or live write occurred.

The specialist stops after commit and push. The Delivery Lead owns PR creation,
review, feedback, merge, and issue closure.
