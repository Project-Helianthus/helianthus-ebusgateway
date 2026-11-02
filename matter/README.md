# Matter SemReg projection

The adopted target is explicitly selected with `Project161(snapshot, evaluation)`.
`Manifest161()` identifies `matter.data-model` version `1.6.1`, mapping revision
`1`, under `helianthus.gateway.matter-projection/v1`. `CanonicalJSON161` checks
that the document, model revisions and complete report manifest agree, that
current values are canonical decimal integers within the signed target limits,
and that attribute presence agrees with the transformed current report row.

The companion [1.6.1 adoption contract](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/targets/matter-1.6.1-v1.md)
and [full ledger](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/targets/matter-1.6.1-v1.json)
are the public contract and must be published before or with this implementation.
The inspected model uses Electrical Sensor `0x0510` revision 1, Power Topology
`0x009C` revision 1, and Electrical Power Measurement `0x0090` revision 3.
These are distinct from the historical 1.7 Sensor/Topology revision 2 identities.
Proven model-source inspection is separate from runtime qualification; full
standards equivalence and physical behavior remain Unknown.

Both entry points share the existing snapshot/evaluation correspondence,
source qualification and exact conversion policy. The only positive mapping is
observed, qualified, promoted, valid, fresh and available `evse.ac.current` from
one eligible phase-qualified source. Amperes become integral milliamperes at
Electrical Power Measurement `ActiveCurrent` attribute `0x0005` only when exact
and within the signed target limits. Fractional milliamperes, ambiguous sources
and invalid inputs refuse the whole payload. Phase and endpoint identity are
explicit irreversible loss. Source evidence remains in the supplied SemReg
snapshot and report source keys; the projector does not mutate or retain it.

The complete ledger covers every field, capability and operation from EVSE
1.0.0, infrastructure 1.0.0, PV 1.0.0, storage 1.1.0 and thermal 1.0.0. All
other rows remain Unknown with explicit non-positive dispositions. Services have
no SemReg projection item kind. `Project161` builds a new accounting report using
the 1.6.1 manifest and the shared full ledger, alongside its 1.6.1 document.

`Project`, `Manifest` and `CanonicalJSON` retain the historical 1.7 draft target.
The HTTP feed in [feedv1](feedv1/contract.go) still consumes that historical API;
calling the new entry point does not cut it over. Historical model/source pins,
feed files and existing golden snapshots are unchanged. Default feed activation
and module/model pin migration require the separate owner migration and complete
accepted target evidence. Nothing is stable before Helianthus 1.0.

This package is pure projection. It allocates no endpoints and has no SDK,
commissioning, fabric, transport, subscription, command dispatch or I/O. Offline
synthetic evidence does not establish go-fabric execution, hosted builds,
Linux arm64, Pi performance, latency, conformance, certification or physical
qualification. Those remain Unknown here.
