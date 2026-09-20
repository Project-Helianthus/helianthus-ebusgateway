# SunSpec To SemReg PV: Positive And Field-Isolated Negative Example

This example traces the existing read-only SunSpec path. It adds no simulator,
native read, semantic implementation, fallback, or live-device claim.

## Exact evidence baseline

- Gateway production baseline:
  [`8e6194e964da043e1806790ebe451fab61e6b588`](https://github.com/Project-Helianthus/helianthus-ebusgateway/commit/8e6194e964da043e1806790ebe451fab61e6b588).
- Focused public example tests:
  [`6f9d36afa5b5633f01d5747414a51f5bb2a198f4`](https://github.com/Project-Helianthus/helianthus-ebusgateway/commit/6f9d36afa5b5633f01d5747414a51f5bb2a198f4).
- Native registry pin:
  [`helianthus-modbusreg ed75fdfbed0d`](https://github.com/Project-Helianthus/helianthus-modbusreg/commit/ed75fdfbed0d42eb2f159afc0174449b545b31af).
- Canonical semantic contract pin:
  [`helianthus-semreg 089ed6ae9004`](https://github.com/Project-Helianthus/helianthus-semreg/commit/089ed6ae9004cfba8aff27f1e54d579aeccc0b4c).

## One path, with native evidence retained

| Stage | Existing owner and immutable proof |
| --- | --- |
| Native acquisition and qualification | Gateway performs bounded read-only acquisition in [`sunspec_producer.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/8e6194e964da043e1806790ebe451fab61e6b588/internal/modbusadapter/sunspec_producer.go). The exact Fronius chain replay and wire/logical provenance are tested in [`sunspec_producer_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/8e6194e964da043e1806790ebe451fab61e6b588/internal/modbusadapter/sunspec_producer_test.go#L20). Model decoding, capability selection, flavor qualification, and the retained observation remain owned by [`helianthus-modbusreg`](https://github.com/Project-Helianthus/helianthus-modbusreg/blob/ed75fdfbed0d42eb2f159afc0174449b545b31af/sunspec_qualification_observation.go). |
| SemReg publication | The existing [14-item native-to-PV map](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/8e6194e964da043e1806790ebe451fab61e6b588/internal/modbusadapter/semreg_pv_publication_core.go#L210) constructs one versioned snapshot, evaluation, selections, and projection report. Each candidate retains source/binding identity, native evidence, lifecycle times, freshness policy, quality, origin, and explicit projection loss. |
| MCP | [`semantic.v1.pv.current.get`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/8e6194e964da043e1806790ebe451fab61e6b588/mcp/modbus_v1.go#L148) returns that same four-object view through the Gateway provider. Its public fixture asserts selected power and withheld frequency at [`6f9d36a`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/6f9d36afa5b5633f01d5747414a51f5bb2a198f4/mcp/semantic_pv_test.go#L45). |
| M2M GraphQL | `SemanticPVCurrent` accepts only the fixed `PUBLIC_GRAPHQL_SEMANTIC_PV_V1` query and returns the same snapshot, evaluation, selections, and projection objects. The exact shape and field isolation are pinned by the [GraphQL test and golden](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/6f9d36afa5b5633f01d5747414a51f5bb2a198f4/m2mgraphql/semantic_pv_test.go#L15). |
| Portal | The Portal PV endpoint forwards the closed M2M GraphQL response; it does not decode registers or select semantic candidates. The [forwarding test](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/6f9d36afa5b5633f01d5747414a51f5bb2a198f4/portal/pv_modbus_red_test.go#L58) proves the selected power and withheld-frequency result is unchanged. |

The consumer fixtures prove the public surfaces preserve the four-object
contract. The SemReg publication tests are the behavioral proof; the consumers
do not reimplement its rules.

## Positive example

`TestPUBLIC05ValidPowerIsAvailableInPublicSemRegView` injects a valid native
active-power value of **1234.5 W** and a valid 50 Hz frequency into the existing
deterministic SunSpec chain fixture. It proves that active power:

- becomes exactly one `pv.ac.aggregate_active_power` quantity in watts;
- is `qualified` and `promoted` with native evidence, binding, and source epoch;
- evaluates `fresh` and remains selected for presentation; and
- has an `exact` projection disposition with one source key and no projection
  loss.

The executable proof is
[`semreg_pv_publication_core_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/6f9d36afa5b5633f01d5747414a51f5bb2a198f4/internal/modbusadapter/semreg_pv_publication_core_test.go#L147).

## Negative example: one invalid field

`TestPUBLIC05InvalidFrequencyIsUnavailableWhilePowerRefreshes` starts with
1234.5 W and 50 Hz, then refreshes the same native identity with **4321.5 W**
and an invalid **2000 Hz** value. The result is deliberately partial:

- the immutable snapshot advances and keeps the complete 14-item projection
  accounting;
- the previous 50 Hz candidate remains as native evidence but evaluates
  `stale`, receives no presentation selection, and is reported `withheld` with
  reason `mapping.field_invalid`;
- the invalid refresh does not create a frequency candidate or invent zero;
- active power advances to revision 2 as exactly 4321.5 W, evaluates `fresh`,
  remains selected, and retains an `exact` no-loss disposition; and
- the complete projection is not replaced, stale frequency is not resurrected
  as available, and no consumer bypasses SemReg.

The executable proof is
[`semreg_pv_publication_core_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/6f9d36afa5b5633f01d5747414a51f5bb2a198f4/internal/modbusadapter/semreg_pv_publication_core_test.go#L362).

Run the focused evidence:

```bash
GOWORK=off go test -race ./internal/modbusadapter ./mcp ./m2mgraphql ./portal \
  -run 'TestPUBLIC05|TestSemanticPVToolReplacesLegacyCanonicalPVTool|TestSemanticPVCurrentUsesOneEvaluatedProjection|TestPortalPVForwardsClosedM2MEnvelopeAndRawReadUsesMCPEnvelope' \
  -count=1
```

This is offline deterministic evidence. It does not establish a packaged add-on
revision, installed consumer, SunSpec certification, exact-device support, or
physical validation.

