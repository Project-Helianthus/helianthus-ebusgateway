# Matter 1.7 SemReg projection

This package implements `helianthus.gateway.matter-projection/v1`: a pure,
deterministic projection from a supplied SemReg snapshot and evaluation to a
detached `matter.data-model` document and the SemReg projection report.

It pins `connectedhomeip` Matter data model `1.7-draft-ballot-0.9` at
`29b4768a513cf566011ab8cd60df1bc495204953` (spec source
`214e40c9d51cfe89050eae68ca5b76238fcfa332`) and mapping revision `1`.
The manifest exactly uses SemReg `089ed6ae9004cfba8aff27f1e54d579aeccc0b4c`
metadata for EVSE 1.0.0, infrastructure 1.0.0, PV 1.0.0, storage 1.1.0, and
thermal 1.0.0.

The accepted mapping contract is pinned to
`helianthus-docs-semantic@30f5e5c79ac6da3a7c7c10c990599906d1dfd0cb`,
`api/v1/targets/matter-1.7-ballot-0.9-v1.json`, SHA-256
`5ae81d5e0971d25ead08f982fdf31caf47ada4838d0ee6f2b3e719d11a6df39c`.

The sole positive v1 mapping is observed, qualified, promoted, fresh and
available `evse.ac.current`: amperes become milliamperes at Electrical Sensor
device type `0x0510`, Electrical Power Measurement cluster `0x0090`,
`ActiveCurrent` attribute `0x0005`. Endpoint and phase identity are explicit,
irreversible loss. Every other field, capability, and operation has an explicit
non-positive ledger disposition. Services have no SemReg projection item kind.

This is not Matter runtime composition. It has no Matter SDK dependency and
creates no Matter node, endpoint allocation, commissioning, fabric, transport,
subscription, I/O, access control, authority decision, intent, route, command
dispatch, command result, or retained state. It makes no certification,
conformance, live-device, or physical-result claim.
