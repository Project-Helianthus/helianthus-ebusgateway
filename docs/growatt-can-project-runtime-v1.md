# Independent Growatt PCS and LINK receive runtimes

The gateway accepts explicit PCS and LINK selections through two independent,
disabled-by-default families. V1.04 keeps its existing flags and behavior.
Each enabled family requires a different CAN interface; all three require three
distinct interfaces. Invalid PCS/LINK admission or duplicate interfaces refuses
the combined CAN configuration before native construction or interface open.
An open or receive failure on a valid, distinct interface affects its own managed
driver and cached binding. Other managed drivers continue independently.

The native owners define interpretation:

- [PCS: Profile selection; Data validity and consumer obligations](https://github.com/Project-Helianthus/helianthus-docs-canbus/blob/main/protocols/growatt/growatt-low-voltage-bms-can-pcs-protocol-v1.md).
- [LINK: Identify the peer before interpreting version-dependent fields; Transport, addressing and data validity](https://github.com/Project-Helianthus/helianthus-docs-canbus/blob/main/protocols/growatt/growatt-low-voltage-bms-can-link-protocol-v1.md).

Selection comes from operator assertions. Identifiers, identity traffic, arrival
order and plausible values do not select a profile, revision, topology or
controller mode. Endpoint, group and member ambiguity remains explicit. Proven
and Hypothesis are candidate protocol grades; Unknown meanings remain raw.
`physical_qualified=false` throughout. Nothing is stable before 1.0.

## Gateway flags

Every flag below starts with `-growatt-bms-pcs-can-` or
`-growatt-bms-link-can-`. Defaults are false, empty strings and zero duration.
Setting active fields while the family is disabled is refused. There is no
implicit stale-age, source profile, route or association default.

| Suffix (both families) | Required value when enabled |
| --- | --- |
| `enabled` | `true` to enable only this family |
| `interface` | Explicit interface name, unique across enabled CAN families |
| `listen-only-confirmed` | `true`: records an external precondition |
| `stale-after` | Positive Go duration, such as `2s` |
| `project-code` | Exact-case project code; LINK uses this only for explicit `use_default` entries |
| `release` | Explicit four-digit release; LINK uses this only for explicit `use_default` entries |
| `native-profile` | `standard`, `extended19` or `parallel`; for LINK this is the peer's PCS profile |
| `scope` | `local` or `aggregate`; LINK requires `local` |
| `bank` | `ordinary` for standard/extended19, `parallel` for parallel |
| `context` | One strict JSON array described below |

PCS additionally accepts `-growatt-bms-pcs-can-native-contract=legacy19` or
`release11` when its native selection needs explicit disambiguation. Unsupported,
unknown, missing or wrong-case profile/release selections are refused.

LINK requires `-growatt-bms-link-can-source-selectors`. The value is one ordered
JSON array with 1 through 32 entries. Each entry has exactly one of these forms:

```json
[{"source":1,"project_code":"QAAA","release":"0011","contract":"release11"}]
```

```json
[{"source":1,"use_default":true,"contract":"release11"}]
```

Contracts are exactly `legacy19` or `release11`. `source` is a unique integer
from 1 through 254, excluding 244 and 245; 255 is also excluded. These are
conservative software admission bounds, not hardware-address claims. Explicit
and default keys cannot mix. A default entry requires the family project/release;
explicit entries can use heterogeneous project/release/contract combinations.
An omitted source never inherits a default. Duplicate keys, duplicate sources,
unknown keys, noninteger sources, nulls, malformed or trailing JSON are refused.
The selector input is bounded to 16 KiB.

### Explicit route and association context

Both `-growatt-bms-pcs-can-context` and `-growatt-bms-link-can-context` accept
one JSON array bounded to 128 KiB. PCS requires exactly one entry. LINK requires
one entry per selector in the same order, with the same `source`. Every key in
the following synthetic example is required, including explicit false validity
and zero values. Keys are case sensitive; unknown, duplicate, null, incorrectly
typed or omitted keys are rejected at every depth, as is trailing JSON.

```json
[
  {
    "source":1,
    "interface_index":7,
    "destination":244,
    "endpoint":"synthetic",
    "group":"group",
    "slot":"slot",
    "epoch":1,
    "serial_slot":1,
    "serial_slot_valid":true,
    "native_slot":1,
    "native_group":1,
    "slot_valid":true,
    "group_valid":true,
    "publisher_resolved":true,
    "measurement_valid":true,
    "temperature_valid":true,
    "topology_associated":true,
    "field_age":"0s",
    "cells":{
      "count":8,
      "count_valid":true,
      "gates":[
        {"configured":true,"valid":true,"status":"","age":"0s"},
        {"configured":true,"valid":true,"status":"","age":"0s"},
        {"configured":true,"valid":true,"status":"","age":"0s"},
        {"configured":true,"valid":true,"status":"","age":"0s"}
      ]
    },
    "sensors":{
      "count":4,
      "count_valid":true,
      "gates":[
        {"configured":true,"valid":true,"status":"","age":"0s"},
        {"configured":true,"valid":true,"status":"","age":"0s"},
        {"configured":true,"valid":true,"status":"","age":"0s"},
        {"configured":true,"valid":true,"status":"","age":"0s"}
      ]
    }
  }
]
```

`interface_index` is a positive explicit OS identity assertion. The receiver
must match both the supplied interface name and index. No interface lookup is
performed by flag parsing or admission. `source`, `destination`, `serial_slot`,
`native_slot` and `native_group` are integer bytes; native admission still
controls their meaning and validity. LINK destination must be 244 or 245.
PCS route inputs are explicit even for standard/parallel; extended PCS receives
an explicit selected route. Direction is fixed by the selected family:
PCS BMS-to-PCS and LINK module-to-coordinator.

`endpoint`, `group` and `slot` are nonempty, nonsecret operator association
labels; `epoch` is a positive integer. Do not supply a device serial or private
identity. Slot/group and serial-slot validity are separate assertions, never
inferred from a label. The accepted LINK config shares one interface identity,
route, profile, bank, endpoint/group/slot and epoch across its ordered source
map. Project/release/contract and the remaining validity inputs can differ by
source. Conflicting common context is refused.

The four `gates` correspond to the native four-word measurement input; they do
not assert complete vector coverage. Cell counts have software bounds 0..16 and
sensor counts 0..8; a valid count must be positive. Zero with `count_valid=false`
remains an explicit unknown count. Each gate has independent `configured`,
`valid`, `status` and nonnegative duration `age`. Status is empty or one of
`absent`, `invalid`, `unsupported`, `stale`, `conflict`. `field_age` is also a
required nonnegative duration. Ages describe assertions at composition time
and increase with elapsed monotonic time, including across receiver restarts;
new traffic does not renew them. A rejected field cannot erase a valid sibling.
Native requirements without supplied evidence, including member-dependent or
derived summary context, remain withheld by the native contract.

## Cached status and safety

Managed identities are `growatt.bms.pcs.can` and `growatt.bms.link.can`, with
separate sources, lifecycle and failure state. Their admitted bindings read only
detached native caches under generation-fenced managed leases. Raw payload,
interface, IDE, DLC/raw DLC, endpoint/group/slot, epoch, sequence, monotonic time,
field age, grade and last-good state stay with the native observations. Zero,
absent, invalid, unsupported, stale and conflict remain distinct. No semantic
cell publication or generic operation/MCP registration is added; eBUS MCP and
GraphQL contracts are unaffected.

`listen-only-confirmed` does not set or verify controller mode. Process behavior
cannot prove physical acknowledgement suppression. The composed interfaces have
receive and close operations only: no transmission, acknowledgement, probing or
bus configuration. Offline fixtures establish software behavior, not physical
qualification.

## Offline classifier

The existing classifier reads local text or stdin without opening a bus:

```sh
go run ./cmd/growatt-can-classify -input synthetic.txt -format json
go run ./cmd/growatt-can-classify -input - -format text < synthetic.txt
```

Use synthetic/public text. Classifier candidates do not populate runtime flags,
resolve missing selectors or qualify a physical interface. No live capture or
hardware procedure is required by this page.
