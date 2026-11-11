# SunSpec To SemReg PV: Positive And Field-Isolated Negative Example

This example traces the existing read-only SunSpec path. It adds no simulator,
native read, semantic implementation, fallback, or live-device claim.

## One path, with native evidence retained

| Stage | Existing owner and immutable proof |
| --- | --- |
| Native acquisition and qualification | Gateway performs bounded read-only acquisition in [`sunspec_producer.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/sunspec_producer.go). The exact Fronius chain replay and wire/logical provenance are tested in [`sunspec_producer_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/sunspec_producer_test.go#L20). Model decoding, capability selection, flavor qualification, and the retained observation remain owned by [`helianthus-modbusreg`](https://github.com/Project-Helianthus/helianthus-modbusreg/blob/main/sunspec_qualification_observation.go). |
| SemReg publication | The existing [14-item native-to-PV map](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L210) constructs one versioned snapshot, evaluation, selections, and projection report. Each candidate retains source/binding identity, native evidence, lifecycle times, freshness policy, quality, origin, and explicit projection loss. |
| MCP | [`semantic_v1_pv_current_get`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/modbus_v1.go#L148) returns that same four-object view through the Gateway provider. Its public fixture asserts selected power and withheld frequency in [`semantic_pv_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/semantic_pv_test.go#L54). |
| M2M GraphQL | `SemanticPVCurrent` accepts only the fixed `PUBLIC_GRAPHQL_SEMANTIC_PV_V1` query and returns the same snapshot, evaluation, selections, and projection objects. The exact shape and field isolation are asserted by the [GraphQL test and golden](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/m2mgraphql/semantic_pv_test.go#L15). |
| Portal | The Portal PV endpoint forwards the closed M2M GraphQL response; it does not decode registers or select semantic candidates. The [forwarding test](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/pv_modbus_red_test.go#L58) proves the selected power and withheld-frequency result is unchanged. |
| Composed runtime proof | [`TestPUBLIC05SunSpecSemRegSurvivesGatewayProvidersAndPortal`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/public05_sunspec_path_test.go#L24) reuses the accepted native SunSpec replay through the real producer, advances an injected lifecycle clock through the production freshness window, publishes the refresh through the real adapter, verifies the Gateway MCP provider retains the exact snapshot, evaluation, selection, and projection state, and then verifies the same result through the mTLS M2M GraphQL runtime and Portal endpoint. |

The consumer fixtures prove the public surfaces preserve the four-object
contract. The composed runtime proof verifies that the real providers retain
that contract. The SemReg publication tests remain the behavioral proof; the
consumers do not reimplement its rules.

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
[`semreg_pv_publication_core_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core_test.go#L148).

## Negative example: one invalid field

`TestPUBLIC05InvalidFrequencyIsUnavailableWhilePowerRefreshes` starts with
1234.5 W and 50 Hz, then refreshes the same native identity with **4321.5 W**
and an invalid **2000 Hz** value. The result is deliberately partial:

- the immutable snapshot advances and keeps the complete 14-item projection
  accounting;
- the previous 50 Hz candidate remains as native evidence but evaluates
  `stale` and `degraded`, receives no presentation selection, and is reported
  `withheld` with reason `mapping.field_invalid`;
- the invalid refresh does not create a frequency candidate or invent zero;
- active power advances to revision 2 as exactly 4321.5 W, evaluates `fresh`,
  remains selected, and retains an `exact` no-loss disposition; and
- the complete projection is not replaced, stale frequency is not resurrected
  as available, and no consumer bypasses SemReg.

The executable proof is
[`semreg_pv_publication_core_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core_test.go#L363).

Run the focused evidence:

```bash
GOWORK=off go test -race ./internal/modbusadapter ./cmd/gateway ./mcp ./m2mgraphql ./portal \
  -run 'TestPUBLIC05|TestSemanticPVToolReplacesLegacyCanonicalPVTool|TestSemanticPVCurrentUsesOneEvaluatedProjection|TestPortalPVForwardsClosedM2MEnvelopeAndRawReadUsesMCPEnvelope' \
  -count=1
```

This is offline deterministic evidence. It does not establish a packaged add-on
revision, installed consumer, SunSpec certification, exact-device support, or
physical validation.
