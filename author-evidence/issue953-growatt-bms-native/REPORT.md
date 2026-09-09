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

Each successful on-demand Growatt MCP call reports `LIVE` consistency and the
RFC3339Nano `ReceiptWall` of its completed immutable four-read envelope as
`data_timestamp`. The production provider carries this receipt with the typed
status; a missing receipt fails closed with the existing unavailable
`RETAINED_PROFILE` envelope and an empty timestamp.

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
provider composition, MCP registration, Portal provider binding, the MCP
observer runtime, and the qualified native MCP projection. A diff that changes
the exact direct `helianthus-modbus` or `helianthus-modbusreg` selection in
`go.mod` also triggers; unrelated module bumps do not. It runs the gateway's
deterministic composition fixtures, then inventories every pinned endpoint test
before running the anchored `^TestRTUProduction` suite. The required f670 inventory
is `ReadRetainsImmutableCorrelatedEvidence`, `ExceptionDoesNotFenceButShortWriteDoes`,
`RejectsUnadmittedReadBeforeWrite`, `RecoveryWaitsForRetiringReadOwnership`,
`FourSequentialReadsRemainBounded`, `CancellationFencesAndPartialFramesRetainEvidence`,
`MalformedAndCRCFramesRemainTerminalEvidence`,
`RejectsTimingAndRecoveryBoundMismatch`,
`RecoveryDiscardsDelayedOldGenerationFrame`, and
`RejectsRecoveryBoundsAndNoByteTimeout`. Missing or duplicate expected names,
gateway failures, and endpoint conformance failures fail closed. The gate tests
prove every composition input triggers, every such trigger fails closed for a
failed command, a `main.go` runtime change runs both the eBUS and RTU gates,
and a non-exempt `config.go` change (including `DefaultConfig` RTU input) runs
both gates; the exact SemReg-PV-only config exemption remains outside both. A
partial endpoint inventory is rejected. T01..T88 remains the required gate for
eBUS transport/topology changes. The documented owner override requires
both its exact token and a scope/residual-risk reason; when present, it clears
every active gate in that invocation, including both gates activated by
`main.go`. No override was used for this issue's CI evidence.

Final configured CI passed:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

It covers `gofmt`, Portal Node `93/93`, assets, vet, native/Linux builds, full
`go test -race ./...`, source-selection schema coverage, Python suites (`168`,
`6`, `22`, `8`, `6`, `2`), `golangci-lint` (`0 issues`), the Modbus RTU
composition/pinned-endpoint transport gate, and the passive smoke gate. The
Modbus RTU gate passed; passive smoke was not triggered. Two prior full CI runs
failed in unchanged `internal/adaptermux` at
`TestManagedConnectionLossLinearizesProxyAdmissionAndProviderUse/blocked_write_drains_before_BACKOFF_publication`
or `blocked_request_start_drains_before_BACKOFF_publication`; retained author
log SHA-256 is `36fdf34009b3984c60c07201cb6c8c47d464d8d7b80b803826fe48bdcb3c6669`
and independent review log SHA-256 is
`7a75aadc412f34801e9c68e92c380adc3df6f278d6151a75dd8f03ac0bb02746`.
The isolated `GOWORK=off go test -race -count=1 ./internal/adaptermux` rerun
passed in `114.416s`; no causal claim is made. A temporary diagnostic test
patch was saved outside this repository and restored before the clean run; it
is not part of this PR. The latest fresh non-concurrent full CI run, with no
owner override, passed with log SHA-256
`d04a6a16a103d829939a5d356759e4fe4d0096d414ea102f4933588c347e6369`.

## Boundary

This is offline composition evidence, not physical qualification, device
compatibility, serial timing proof, deployment, or hardware operation. A future
authorized hardware qualification and separate SemReg cutover remain required.
No credentials, device I/O, deployment, service action, or live write occurred.

The specialist stops after commit and push. The Delivery Lead owns PR creation,
review, feedback, merge, and issue closure.
