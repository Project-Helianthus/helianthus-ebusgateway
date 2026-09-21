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

The sole positive mapping is observational `evse.ac.current`: qualified,
promoted, fresh, available amperes convert to integer milliamperes at composed
Electrical Sensor device type `0x0510`, Electrical Power Measurement cluster
`0x0090`, `ActiveCurrent` attribute `0x0005`. The output records irreversible
endpoint and phase identity loss. A value with sub-milliampere precision fails
closed. All other fields, capabilities, and operations appear exactly once in
the SemReg report with explicit `unknown` disposition. SemReg services have no
projection-item kind, so the adapter does not invent service rows.

The adapter has no endpoint allocation, commissioning, fabric, transport,
subscription, I/O, native reads, route selection, retained state, authority,
intent, command dispatch, command result, or live-device behavior. It is not
Matter certification or a runtime composition claim. The semantic documentation
contract tracked by `helianthus-docs-semantic#31` remains a required merge gate.
