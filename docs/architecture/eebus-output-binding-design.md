# eeBUS output binding: ownership and loss contract (design)

Status: reviewable application design for issue #1425, a partial slice of
Project #2 INT-04 / INT-12. It is not an implementation, not a normative
SemReg-to-eeBUS mapping and not INT-12 completion. Nothing here is stable
before 1.0, and every MCP namespace stays pre-stable.

An *eeBUS output binding* is the direction opposite to eeBUS inbound promotion:
it takes already published SemReg state and prepares a target document that an
eeBUS-side owner could serve to a partner. This page fixes the questions a
later implementation must answer and the boundaries it may not cross. It
decides none of the open ones implicitly.

The page is split into three independent axes that must not be collapsed:

1. [Ownership decision](#1-ownership-decision) — which repository owns the
   binding. **Unresolved.**
2. [Target mapping evidence](#2-target-mapping-evidence) — which SemReg item
   may map to which eeBUS element. **Unknown** for every item until STD-01.
3. [Implementation readiness](#3-implementation-readiness) — what a skeleton
   may already do, and what it may never claim.

## Existing owners this design does not move

| Concern | Current owner | Source |
| --- | --- | --- |
| SHIP/SPINE wire semantics, use-case meaning, native mapping | [Public eeBUS documentation](https://github.com/Project-Helianthus/helianthus-docs-eebus) | Native docs contract; STD-01 revisions are unresolved. |
| eeBUS discovery, trust, pairing, sessions, raw topology, native writes and rollback | [eeBUS registry runtime](https://github.com/Project-Helianthus/helianthus-eebusreg) through the gateway's managed driver | [`eebus_runtime_adapter.go`](../../cmd/gateway/eebus_runtime_adapter.go), [managed sidecar composition](managed-sidecar-composition.md) |
| Raw eeBUS command policy (read delegation, write-authorization validation, mutation-lab gating) | Gateway [`internal/eebuscommand`](../../internal/eebuscommand/router.go) | Owner-only `eebus.v1.features.data.set` / `eebus.v1.mutations.*` stay unavailable without the existing exact write authorization and profile. |
| Operator pairing and admin HTTP boundary | Gateway [`internal/eebusadmin`](../../internal/eebusadmin/server.go) under the canonical protocol contract | [Shared consumer boundary](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/eebus-operator-admin.md) |
| Promoted protocol-neutral types, evaluation, projection accounting, operation admission | [SemReg](https://github.com/Project-Helianthus/helianthus-semreg) | [Kernel contract](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/kernel.md) |
| Shared detached snapshot read and its disposition vocabulary (M07) | Docs contract, gateway [`internal/semanticview`](../../internal/semanticview/view.go) | [Shared SemReg snapshot read](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/semantic-snapshot-read.md) |
| Generic semantic operation admission and dispatch to an injected native executor | Gateway [`internal/semanticoperations`](../../internal/semanticoperations/router.go) | [One semantic operation route](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/semantic-operation-route.md) |

The binding is a *consumer* of the shared read and a *caller* of the operation
route. It owns no trust, session, pairing, native write, ACK, readback,
authorization tier or protocol-safety decision. It adds no authorization gate:
eeBUS trust stays with the registry, and operation admission stays with SemReg
and the injected native executor.

## 1. Ownership decision

**Decision status: unresolved.** The repository owner has not decided which
repository owns the binding. This page records the options and the criteria;
merging it does not choose one.

| Option | Location | For | Against |
| --- | --- | --- | --- |
| A (proposed) | Gateway `internal/eebusoutput` (new package, not yet created) | Same composition layer as the existing [Matter output binding](matter-output-binding-v1.md) and the shared read; reuses the gateway-held coherent capture and operation router without new cross-repository APIs. | The gateway is not the semantic owner; it must not grow eeBUS protocol meaning. Requires an eeBUS-registry interface to serve a local feature that is not known to exist. |
| B | eeBUS registry | Owns SPINE feature serving, trust and sessions already; protocol knowledge stays in one place. | The registry would need a SemReg dependency and projection accounting; it must stay free of semantic promotion logic. |
| C | SemReg target contract plus thin gateway adapter | Mapping tables live with other projection targets in the semantic docs. | A target contract cannot be authored before STD-01 resolves native meaning. |

Criteria the decision must satisfy, whichever option wins:

- Protocol-native meaning, identity, trust and safety remain in the eeBUS
  owner; semantic meaning remains in SemReg. The binding translates between
  already-accepted contracts only.
- The chosen owner can be tested offline with synthetic fixtures and no
  network, partner or device.
- The decision is recorded explicitly by the repository owner in a follow-up
  issue; an implementation PR that merely creates a package does not decide it.

## 2. Target mapping evidence

Every mapping row has an evidence conclusion (`Proven`, `Hypothesis`,
`Unknown`) that is separate from runtime qualification (`candidate`,
`qualified`, `unsupported`, `unknown`, `rejected`) and from promotion.

| Axis | Current state |
| --- | --- |
| Native target meaning (use case, feature, function, element) | **Unknown.** STD-01 revisions are unresolved; no public native docs contract exists to cite. |
| SemReg source items | Existing pack definitions for PV, Storage, EVSE, Thermal and Infrastructure. Their existence is not evidence of an eeBUS counterpart. |
| eeBUS inbound family state | `eebus-inbound` stays `unknown` with reason `unknown_pending_std_01` in the SemReg family catalog; the shared view preserves that token verbatim. |
| Normative mapping rows | **None frozen.** No row is `Proven`; a candidate row may be recorded only as `Hypothesis` with its public source, never projected as `exact` or `transformed`. |

Consequences for the binding until the native docs contract cites STD-01
outcomes:

- No target element, unit, scale, identifier or use-case binding is defined.
- A readable SemReg fact, a connected partner or a successful raw inspection
  does not establish a mapping, qualification or conformance.
- The target identifier and target version are placeholders; they are not
  published identifiers.

## 3. Implementation readiness

### Proposed compiled adapter interface

Shape only; names are proposals for option A and carry no stability promise.

```go
package eebusoutput

// Input is one detached selection from the shared coherent capture.
type Input struct {
	Snapshot   semreg.Snapshot
	Evaluation semreg.EvaluationView
}

// Result pairs the SemReg projection report with the target document.
// Document stays empty until a native mapping is Proven.
type Result struct {
	Report   projection.ProjectionReport
	Document Document
}

// Adapter is pure: no I/O, no native read, no session, no send.
type Adapter interface {
	Manifest() projection.ProjectionManifest
	Project(Input) (Result, error)
}
```

The adapter is compiled in, has no plugin loading and no eeBUS SDK dependency.
It delegates completeness and reference validation to SemReg
`projection.Project`, as the [shared view](../../internal/semanticview/view.go)
and the [Matter projection](../../matter/projection.go) already do.

### Selected input snapshot

- One call to the shared coherent capture (the `SnapshotCapture` seam declared
  in [`internal/semanticoperations`](../../internal/semanticoperations/router.go))
  per projection. It selects only the current admitted source epoch and driver
  generation of each asset and applies the public-exposure and lifecycle gates
  of the M07 contract before returning.
- An asset entry withheld by the capture
  (`semantic.read.reason.publication_withheld`, `asset_unknown`,
  `snapshot_unavailable`, `source_unavailable`) is not projected at all; the
  binding does not strip records, re-read a native source or fall back to
  cached state.
- A projection never mixes two captures. A later publication produces a new
  projection; partial failure of one asset does not replace another asset's
  last accepted projection.

### Projection loss

The binding reuses the SemReg disposition vocabulary of the M07 contract
unchanged: `exact`, `transformed`, `withheld`, `unrepresentable`,
`unsupported`, `unknown`, and loss kinds `unit`, `range`, `precision`, `time`,
`symbol`, `provenance`, `identity`, `capability`, `operation`, `policy`.

Rules specific to the output direction:

- Every requested item appears exactly once. Before STD-01, with no item
  `Proven`, the only permitted outcomes are `withheld`, `unsupported` (with
  explicit supported evidence) and `unknown`.
- Source reasons are preserved verbatim. `unknown_pending_std_01` keeps
  precedence and is never prefixed or reinterpreted; candidate and unqualified
  capabilities stay `withheld` with the owner's reason or
  `semantic.read.reason.capability_unqualified`.
- Unknown items never acquire a synthetic value, zero, empty symbol or default.
- Any future `transformed` row must record each irreversible loss, for example
  asset/endpoint `identity` loss when several SemReg instances would collapse
  into one eeBUS entity; that collapse is rejected unless the loss is recorded.

Synthetic withheld example (identifiers and target values are illustrative
placeholders, not published identifiers):

```json
{
  "report": {
    "contract": "helianthus.semantic.projection/v1",
    "manifest": {
      "target_id": "target:eebus-output-design-placeholder",
      "target_version": "0",
      "kernel_version": "helianthus.semantic.kernel/v1",
      "pack_versions": [{"id": "helianthus.pack.thermal", "version": "1.0.0"}],
      "mapping_revision": "1"
    },
    "requested": [
      {"item_id": "thermal.example.capability", "kind": "capability"},
      {"item_id": "thermal.example.fact", "kind": "fact"}
    ],
    "dispositions": [
      {
        "kind": "capability",
        "item_id": "thermal.example.capability",
        "outcome": "withheld",
        "source_keys": [],
        "loss": [],
        "reason": "semantic.read.reason.capability_unqualified"
      },
      {
        "kind": "fact",
        "item_id": "thermal.example.fact",
        "outcome": "unknown",
        "source_keys": [
          {
            "pack_id": "helianthus.pack.thermal",
            "pack_version": "1.0.0",
            "fact_id": "thermal.example.fact",
            "dimensions": []
          }
        ],
        "loss": [],
        "reason": "unknown_pending_std_01"
      }
    ]
  },
  "document": {}
}
```

`snapshot_id` and `revisions` are omitted from the excerpt; a real report
carries the values of the one captured snapshot.

The capability is withheld because the source instance is only a candidate;
the fact keeps the inbound family reason. The target document is empty: a
withheld or unknown item never appears in it.

### Operation relay boundary

If an eeBUS partner later requests a change through a served feature, the
binding may only translate that request into one closed
[`semanticoperations.Request`](../../internal/semanticoperations/router.go)
and return the router's `Result` status unchanged (`accepted`,
`protocol_ack`, `readback_confirmed`, `failed`, `unknown`).

- SemReg admission, intent ownership, guard re-check, native dispatch, ACK and
  readback evidence stay with the router, the kernel and the injected executor
  of the qualified native owner.
- The binding constructs no native frame, selects no executor, adds no retry,
  reroute or read-after-write sequence, and never reports an eeBUS-side
  acknowledgement as native execution.
- An `UNAVAILABLE` or failed result is relayed as a refusal; it is never
  converted into success. Without an admitted route the relay is unwired.
- Raw `eebus.v1.features.data.set` and mutation rollback remain owner-only
  operations of `internal/eebuscommand`; the binding never calls them.

### Unsupported behavior before STD-01

| Behavior | Before STD-01 |
| --- | --- |
| Serving any eeBUS feature, use case or entity | Not implemented. |
| Positive mapping row | None; every item `unknown` or `withheld` (or `unsupported` with explicit evidence). |
| Operation relay | Unwired; no request reaches the router. |
| New MCP tool, GraphQL field or namespace | None. Existing `eebus.v1.*` and `ebus.v1.*` surfaces are unchanged. |
| Authorization, trust or pairing change | None. |

A skeleton under option A may compile the interface, emit the ledger above and
report `unknown`/`unsupported` truthfully. It may not claim INT-12 normative
completion, conformance, certification, live-partner interoperability or
hardware acceptance.

## Acceptance for a follow-up implementation

- The ownership decision is recorded by the repository owner first.
- Every mapping row cites the public native docs contract and carries its own
  evidence conclusion; only `Proven` rows may produce `exact` or `transformed`.
- Offline synthetic tests cover the full ledger, the withheld/unknown example
  above, verbatim reason preservation, capture-withheld assets and the relay's
  unchanged result statuses.
