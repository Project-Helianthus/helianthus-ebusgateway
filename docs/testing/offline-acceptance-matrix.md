# Offline acceptance matrix

This matrix is the offline step of the release before the hardware session. It records which test of this
repository proves each device family on each surface, on the graph axis and under each condition, through the
composed gateway at the candidate's bill of materials. It is offline composition evidence only: it reports no
hardware result and qualifies no device. The hardware session follows the
[consolidated candidate handoff](consolidated-candidate-handoff.md) and the
[hardware acceptance catalog](hardware-acceptance-catalog.md).

The machine-readable source is [`offline-acceptance-matrix.json`](offline-acceptance-matrix.json), format
`helianthus-offline-acceptance-matrix/1`. The tables at the end of this page are generated from it by
`python3 scripts/offline_acceptance_matrix.py render`, and `python3 scripts/offline_acceptance_matrix.py check`
fails when they differ, so the page and the matrix cannot drift.

## Axes

1. **Family x surface.** Every device family by native MCP, semantic MCP, GraphQL, the Portal, the Home Assistant
   consumer contract, the Prometheus semantic families, the Prometheus native point families, the eeBUS output
   binding and the Matter output binding. Families are device families, never models.
2. **Family x graph.** Per family: canonical resources, relation instances, plane memberships, lens retrieval
   parity across GraphQL, semantic MCP and the Portal, and target reports that cite their lens.
3. **Family x condition.** Normal, degraded, reset, partial failure, stale generation, identity conflict, unknown
   capability and cleanup, each its own cell with its own explicit result; none is implied by a passing driver
   test.
4. **Graph negatives**, checked once across families: duplicate identity, false join, missing membership, stale
   relation, dangling endpoint, identity drift across planes and restart, unauthorized control.

## Cell states

A cell is `{state, test, reason, issue}`.

| State | Meaning | Rule |
|---|---|---|
| `not_run` | No evidence yet. | Names no test. Blocks closing the release's offline acceptance. |
| `pass` | A named test of this repository exercises the cell through the composed gateway (the managed setup entry point and lifecycle, with a replayed or synthetic source), not an injected provider alone. | `test` names a test that exists. |
| `not_applicable` | The cell cannot exist for this family. | Needs a one-line `reason`. Names no test. |
| `not_in_0_7` | The cell is recorded as outside the release. | Needs a one-line `reason` and the owner `issue` as `<repo>#<number>`. Names no test. |

Driver or module readiness alone never fills a cell. Missing real acquisition in the composition leaves the cell
`not_run`, and the candidate is software-incomplete.

A Go test is named as `<package directory> <TestName>` and exists when a `_test.go` file in that directory
declares the `go test` entry point `func <TestName>(t *testing.T)`, where `TestName` is `Test` or `Test` followed
by a character that is not a lower-case letter, and that file is part of the build of the release target: `linux`
on every release architecture (`amd64`, `arm64`, `arm`, `386`), with and without the race detector. A file that a
file-name suffix or a `//go:build` (or legacy `// +build`) constraint excludes from any of those builds, or whose
name starts with `_` or `.`, is never evidence; a custom build tag is unset. A Python test is named as
`<script path> <test_name>` and exists when `test_name` starts with `test` and the test runner collects it: a
module-level `def <test_name>(`, or a method of a module-level class the runner collects (a subclass of
`unittest.TestCase`, or a class named `Test...` without `__init__`). A `test_` function nested inside another
function or inside an uncollected class is never a test. A helper declared in a test file is never a test, and a
declaration-like line inside a comment or a string literal is never a declaration. Test names describe behaviour;
normative conformance identifiers stay in the tests as traceability only and claim no certification.

A `pass` cell in this matrix names a test that starts the gateway through its lifecycle entry point (`run`) or
composes the managed drivers through the managed setup (`prepareManagedSidecars` with the driver manager), runs
the family's own acquisition against a replayed or synthetic source, and asserts that cell's surface or condition
itself. A test that composes the parts by hand, injects a factory that publishes facts, or reads a provider
without its surface leaves the cell `not_run`.

## How a cell is filled

The pull request that adds or names the proving test edits the cell (`state`, `test`) in the same change and
regenerates the tables below with the `render` command. The validator fails when an axis member has no cell, when
a cell names an unknown member, when a member or cell appears twice, when a `pass` cell names no test or a test
that does not exist, when a `not_applicable` or `not_in_0_7` cell has no one-line reason, when a `not_in_0_7` cell
has no owner issue, and when the tables below differ from the matrix. The local gate runs the validator and its
tests.

`python3 scripts/offline_acceptance_matrix.py check --require-complete` also fails on any `not_run` cell and
prints the count per axis. It is run once, by hand, against the frozen candidate when the release's offline
acceptance is closed; it is not part of the local gate.

## Generated tables

<!-- offline-acceptance-matrix:begin (generated by scripts/offline_acceptance_matrix.py render) -->
### Cell counts

| Axis | Cells | `not_run` | `pass` | `not_applicable` | `not_in_0_7` |
|---|---|---|---|---|---|
| `family_surface` | 135 | 117 | 9 | 0 | 9 |
| `family_graph` | 75 | 70 | 0 | 0 | 5 |
| `family_condition` | 120 | 87 | 25 | 0 | 8 |
| `graph_negative` | 7 | 7 | 0 | 0 | 0 |
| total | 337 | 281 | 34 | 0 | 22 |

### Families

| Family | Title | Release |
|---|---|---|
| `ebus_vaillant` | eBUS heating system with a vendor system controller | `in_0_7` |
| `ebus_heat_source_only` | eBUS heat source without a system controller | `in_0_7` |
| `eebus_energy_manager` | eeBUS peer (SHIP and SPINE) | `in_0_7` |
| `sunspec_inverter` | SunSpec inverter family over Modbus TCP | `in_0_7` |
| `huawei_smartlogger` | Huawei SmartLogger gateway over Modbus TCP | `in_0_7` |
| `huawei_sdongle` | Huawei S-Dongle gateway over Modbus TCP | `in_0_7` |
| `huawei_energy_manager` | Huawei energy manager over Modbus TCP | `withheld_by_contract` |
| `growatt_inverter_rtu` | Growatt inverter family over Modbus RTU | `in_0_7` |
| `growatt_bms_rs485` | Growatt battery management over RS-485 | `in_0_7` |
| `growatt_bms_can` | Growatt low-voltage battery management over CAN | `in_0_7` |
| `growatt_bms_pcs_can` | Growatt battery management PCS link over CAN | `in_0_7` |
| `growatt_bms_link_can` | Growatt battery management LINK link over CAN | `in_0_7` |
| `tesla_wall_connector` | Tesla wall connector family | `in_0_7` |
| `outback_power` | OutBack power system over Modbus TCP | `in_0_7` |
| `gree_vrf_can` | Gree VRF over CAN, receive-only | `in_0_7` |

### Surfaces

| Surface | Title |
|---|---|
| `native_mcp` | Native MCP (raw and typed native points) |
| `semantic_mcp` | Semantic MCP |
| `graphql` | GraphQL |
| `portal` | Portal contribution and navigation |
| `home_assistant` | Home Assistant consumer contract (replayed against the gateway surface) |
| `prometheus_semantic` | Prometheus semantic families |
| `prometheus_native` | Prometheus native point families (optional export) |
| `eebus_output` | eeBUS output binding |
| `matter_output` | Matter output binding |

### Graph rows

| Graph row | Title |
|---|---|
| `resources` | Canonical resources published with stable identity |
| `relations` | Relation instances published with both endpoints |
| `plane_membership` | Plane memberships equal the catalog for the resource kind |
| `lens_parity` | Lens retrieval returns the same resources and relations through GraphQL, semantic MCP and the Portal |
| `target_reports` | Target reports cite the lens they were produced from |

### Conditions

| Condition | Title |
|---|---|
| `normal` | Normal acquisition and publication |
| `degraded` | Degraded source: explicit state, no invented value |
| `reset` | Source reset or reconnect: generation fenced, publication resumes |
| `partial_failure` | One instance fails, the others and the HTTP surfaces continue |
| `stale_generation` | A late result of a replaced generation is discarded |
| `identity_conflict` | Conflicting identity evidence withholds admission |
| `unknown_capability` | An unknown capability stays unknown on every surface |
| `cleanup` | Stop releases resources and leaves no publication behind |

### Family x surface

| Family | `native_mcp` | `semantic_mcp` | `graphql` | `portal` | `home_assistant` | `prometheus_semantic` | `prometheus_native` | `eebus_output` | `matter_output` |
|---|---|---|---|---|---|---|---|---|---|
| `ebus_vaillant` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `ebus_heat_source_only` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `eebus_energy_manager` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `sunspec_inverter` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `huawei_smartlogger` | `not_run` | `pass` | `pass` | `pass` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `huawei_sdongle` | `not_run` | `pass` | `pass` | `pass` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `huawei_energy_manager` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` |
| `growatt_inverter_rtu` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_rs485` | `pass` | `pass` | `pass` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_pcs_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_link_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `tesla_wall_connector` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `outback_power` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `gree_vrf_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |

### Family x graph

| Family | `resources` | `relations` | `plane_membership` | `lens_parity` | `target_reports` |
|---|---|---|---|---|---|
| `ebus_vaillant` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `ebus_heat_source_only` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `eebus_energy_manager` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `sunspec_inverter` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `huawei_smartlogger` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `huawei_sdongle` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `huawei_energy_manager` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` |
| `growatt_inverter_rtu` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_rs485` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_pcs_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_link_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `tesla_wall_connector` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `outback_power` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `gree_vrf_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |

### Family x condition

| Family | `normal` | `degraded` | `reset` | `partial_failure` | `stale_generation` | `identity_conflict` | `unknown_capability` | `cleanup` |
|---|---|---|---|---|---|---|---|---|
| `ebus_vaillant` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `ebus_heat_source_only` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `eebus_energy_manager` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `sunspec_inverter` | `not_run` | `not_run` | `not_run` | `pass` | `not_run` | `not_run` | `not_run` | `not_run` |
| `huawei_smartlogger` | `pass` | `not_run` | `pass` | `not_run` | `not_run` | `pass` | `pass` | `pass` |
| `huawei_sdongle` | `pass` | `not_run` | `pass` | `not_run` | `not_run` | `pass` | `pass` | `pass` |
| `huawei_energy_manager` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` | `not_in_0_7` |
| `growatt_inverter_rtu` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_rs485` | `pass` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `pass` |
| `growatt_bms_can` | `pass` | `pass` | `not_run` | `not_run` | `pass` | `not_run` | `not_run` | `not_run` |
| `growatt_bms_pcs_can` | `pass` | `pass` | `not_run` | `not_run` | `not_run` | `pass` | `not_run` | `pass` |
| `growatt_bms_link_can` | `pass` | `pass` | `not_run` | `not_run` | `pass` | `pass` | `not_run` | `pass` |
| `tesla_wall_connector` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `outback_power` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |
| `gree_vrf_can` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` | `not_run` |

### Graph negatives

| Graph negative | Title | State |
|---|---|---|
| `duplicate_identity` | Two sources of one device do not produce two resources | `not_run` |
| `false_join` | Two devices are not joined into one resource | `not_run` |
| `missing_membership` | A resource without a plane membership is refused, not published | `not_run` |
| `stale_relation` | A relation from a retired generation is not served as current | `not_run` |
| `dangling_endpoint` | A relation whose endpoint is absent is not published | `not_run` |
| `identity_drift` | A resource keeps one identity across planes and across a restart | `not_run` |
| `unauthorized_control` | Visibility in a plane grants no operation: a write on a resource without an admitted operation is refused before the bus | `not_run` |

### Cells with a test, a reason or an owner issue

| Cell | State | Test | Reason | Owner issue |
|---|---|---|---|---|
| family_surface huawei_smartlogger x semantic_mcp | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_surface huawei_smartlogger x graphql | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_surface huawei_smartlogger x portal | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_surface huawei_sdongle x semantic_mcp | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_surface huawei_sdongle x graphql | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_surface huawei_sdongle x portal | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_surface huawei_energy_manager x native_mcp | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x semantic_mcp | `not_in_0_7` |  | Semantic publication of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits acquisition; the composed instance publishes nothing. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x graphql | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x portal | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x home_assistant | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x prometheus_semantic | `not_in_0_7` |  | Semantic publication of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits acquisition; the composed instance publishes nothing. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x prometheus_native | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x eebus_output | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_surface huawei_energy_manager x matter_output | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_surface growatt_bms_rs485 x native_mcp | `pass` | cmd/gateway TestM57LifecycleToolsReplaceGenerationsAndRefuseLatePublication |  |  |
| family_surface growatt_bms_rs485 x semantic_mcp | `pass` | cmd/gateway TestM57ComposedDriversShareOneNorthboundPath |  |  |
| family_surface growatt_bms_rs485 x graphql | `pass` | cmd/gateway TestM57ComposedDriversShareOneNorthboundPath |  |  |
| family_graph huawei_energy_manager x resources | `not_in_0_7` |  | Semantic publication of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits acquisition; the composed instance publishes nothing. | helianthus-modbusreg#199 |
| family_graph huawei_energy_manager x relations | `not_in_0_7` |  | Semantic publication of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits acquisition; the composed instance publishes nothing. | helianthus-modbusreg#199 |
| family_graph huawei_energy_manager x plane_membership | `not_in_0_7` |  | Semantic publication of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits acquisition; the composed instance publishes nothing. | helianthus-modbusreg#199 |
| family_graph huawei_energy_manager x lens_parity | `not_in_0_7` |  | Semantic publication of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits acquisition; the composed instance publishes nothing. | helianthus-modbusreg#199 |
| family_graph huawei_energy_manager x target_reports | `not_in_0_7` |  | Semantic publication of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits acquisition; the composed instance publishes nothing. | helianthus-modbusreg#199 |
| family_condition sunspec_inverter x partial_failure | `pass` | cmd/gateway TestH08DialFailureOnOneInstanceLeavesTheOtherRunning |  |  |
| family_condition huawei_smartlogger x normal | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_smartlogger x reset | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_smartlogger x identity_conflict | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_smartlogger x unknown_capability | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_smartlogger x cleanup | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_sdongle x normal | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_sdongle x reset | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_sdongle x identity_conflict | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_sdongle x unknown_capability | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_sdongle x cleanup | `pass` | cmd/gateway TestH11ComposedHuaweiPathOffline |  |  |
| family_condition huawei_energy_manager x normal | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition huawei_energy_manager x degraded | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition huawei_energy_manager x reset | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition huawei_energy_manager x partial_failure | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition huawei_energy_manager x stale_generation | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition huawei_energy_manager x identity_conflict | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition huawei_energy_manager x unknown_capability | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition huawei_energy_manager x cleanup | `not_in_0_7` |  | Acquisition of this family is withheld by its native contract (NO_ADMITTED_CHILD_PROFILE) until that contract admits it; the composed instance is never dialled and publishes nothing, so the cell cannot be exercised in the release. | helianthus-modbusreg#199 |
| family_condition growatt_bms_rs485 x normal | `pass` | cmd/gateway TestM57ComposedDriversShareOneNorthboundPath |  |  |
| family_condition growatt_bms_rs485 x cleanup | `pass` | cmd/gateway TestM57ComposedDriversShareOneNorthboundPath |  |  |
| family_condition growatt_bms_can x normal | `pass` | cmd/gateway TestCX22ComposedThreeFamiliesAndTwoProviderIsolation |  |  |
| family_condition growatt_bms_can x degraded | `pass` | cmd/gateway TestCAN03ReceiveFailureReachesLifecycleAndRestart |  |  |
| family_condition growatt_bms_can x stale_generation | `pass` | cmd/gateway TestCAN03ReceiveFailureReachesLifecycleAndRestart |  |  |
| family_condition growatt_bms_pcs_can x normal | `pass` | cmd/gateway TestCX22ComposedThreeFamiliesAndTwoProviderIsolation |  |  |
| family_condition growatt_bms_pcs_can x degraded | `pass` | cmd/gateway TestCX22LossWithdrawsAndQuarantines |  |  |
| family_condition growatt_bms_pcs_can x identity_conflict | `pass` | cmd/gateway TestCX22NativeContextDoesNotInventTopology |  |  |
| family_condition growatt_bms_pcs_can x cleanup | `pass` | cmd/gateway TestCX22LossWithdrawsAndQuarantines |  |  |
| family_condition growatt_bms_link_can x normal | `pass` | cmd/gateway TestCX22ComposedThreeFamiliesAndTwoProviderIsolation |  |  |
| family_condition growatt_bms_link_can x degraded | `pass` | cmd/gateway TestCX22LossWithdrawsAndQuarantines |  |  |
| family_condition growatt_bms_link_can x stale_generation | `pass` | cmd/gateway TestCX22ComposedThreeFamiliesAndTwoProviderIsolation |  |  |
| family_condition growatt_bms_link_can x identity_conflict | `pass` | cmd/gateway TestCX22SourceReuseAndIncompleteIdentity |  |  |
| family_condition growatt_bms_link_can x cleanup | `pass` | cmd/gateway TestCX22ComposedThreeFamiliesAndTwoProviderIsolation |  |  |
<!-- offline-acceptance-matrix:end -->
