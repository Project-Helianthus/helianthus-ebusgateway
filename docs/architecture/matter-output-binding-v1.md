# Matter 1.7 output binding v1

Issue #995 introduces `helianthus.gateway.matter-projection/v1`, a pure
SemReg-to-Matter data-model adapter. It receives a caller-supplied immutable
SemReg `Snapshot` and matching `EvaluationView`, emits a sorted detached target
document, and delegates accounting validation to SemReg `projection.Project`.

The target is `matter.data-model` version
`1.7-draft-ballot-0.9+29b4768a513cf566011ab8cd60df1bc495204953`; the data-model
commit is `29b4768a513cf566011ab8cd60df1bc495204953`, spec source is
`214e40c9d51cfe89050eae68ca5b76238fcfa332`, and mapping revision is `1`.
The manifest derives, mechanically, from SemReg `089ed6ae9004cfba8aff27f1e54d579aeccc0b4c`
metadata for EVSE 1.0.0, infrastructure 1.0.0, PV 1.0.0, storage 1.1.0, and
thermal 1.0.0.

The accepted public mapping contract is pinned to
`Project-Helianthus/helianthus-docs-semantic@30f5e5c79ac6da3a7c7c10c990599906d1dfd0cb`,
path `api/v1/targets/matter-1.7-ballot-0.9-v1.json`, SHA-256
`5ae81d5e0971d25ead08f982fdf31caf47ada4838d0ee6f2b3e719d11a6df39c`.

The sole positive mapping is observational `evse.ac.current`: qualified,
promoted, fresh, available amperes convert to integer milliamperes at composed
Electrical Sensor device type `0x0510`, Electrical Power Measurement cluster
`0x0090`, `ActiveCurrent` attribute `0x0005`. The output records irreversible
endpoint and phase identity loss. A value with sub-milliampere precision fails
closed. All other fields, capabilities, and operations appear exactly once in
the SemReg report with explicit `unknown` disposition. SemReg services have no
projection-item kind, so the adapter does not invent service rows.

The adapter has no Matter SDK dependency and creates no Matter node, endpoint
allocation, commissioning, fabric, transport, subscription, I/O, native read,
access control, route, retained state, authority, intent, command dispatch, or
command result. It makes no certification, conformance, live-device, or physical
claim. The semantic documentation contract from `helianthus-docs-semantic#31`
is the accepted implementation input pinned above; this package does not redefine
its ownership or turn it into runtime evidence.
