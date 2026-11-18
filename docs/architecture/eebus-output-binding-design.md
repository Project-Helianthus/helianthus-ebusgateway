# eeBUS output binding: ownership and gateway boundary (design)

Status: application design for the gateway side of the eeBUS output binding,
a partial slice of Project #2 INT-04 / INT-12. It is not a normative
SemReg-to-eeBUS mapping and not INT-12 completion. Nothing here is stable
before 1.0, and every MCP namespace stays pre-stable.

An *eeBUS output binding* is the direction opposite to eeBUS inbound promotion:
it takes already published SemReg state and serves it to an eeBUS peer. This
page records where the binding lives and the boundary the gateway keeps around
it.

The page has three parts:

1. [Ownership](#1-ownership) — the binding lives in the eeBUS registry.
2. [Target mapping](#2-target-mapping) — mapping rows are owned by the
   registry and the public eeBUS documentation, not by the gateway.
3. [Gateway boundary](#3-gateway-boundary) — what the gateway hands the
   binding and what it relays back.

## Existing owners this design does not move

| Concern | Current owner | Source |
| --- | --- | --- |
| SHIP/SPINE wire semantics, use-case meaning, native mapping | [Public eeBUS documentation](https://github.com/Project-Helianthus/helianthus-docs-eebus) | Native docs contract, including its output pages. |
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

## 1. Ownership

The repository owner placed the eeBUS output binding in the
[eeBUS registry](https://github.com/Project-Helianthus/helianthus-eebusreg).
The native revisions the binding depends on are recorded in the output pages of
the [public eeBUS documentation](https://github.com/Project-Helianthus/helianthus-docs-eebus);
the gateway does not restate them.

- Protocol-native meaning, identity, trust and safety remain in the eeBUS
  owner; semantic meaning remains in SemReg. The binding translates between
  already-accepted contracts only.
- The gateway owns no mapping table, target identifier, target version or
  target document for eeBUS.
- The binding is tested offline by its owner with synthetic fixtures and no
  network, peer or device.

## 2. Target mapping

Every mapping row carries an evidence conclusion (`Proven`, `Hypothesis`,
`Unknown`) that is separate from runtime qualification (`candidate`,
`qualified`, `unsupported`, `unknown`, `rejected`) and from promotion. The rows,
their evidence and the native element each row reaches are defined by the
output pages of the public eeBUS documentation and implemented by the eeBUS
registry.

The gateway neither adds nor refuses a row:

- A readable SemReg fact, a connected peer or a successful raw inspection does
  not establish a mapping, qualification or conformance.
- The shared view's disposition vocabulary (`exact`, `transformed`,
  `withheld`, `unrepresentable`, `unsupported`, `unknown`) and its loss kinds
  are passed through unchanged; source reasons such as
  `unknown_pending_normative_source` keep precedence and are never prefixed or
  reinterpreted.
- Unknown items never acquire a synthetic value, zero, empty symbol or default.

## 3. Gateway boundary

### Lens results handed to the binding

- The gateway hands the binding lens results whose values come from the graph
  read service. Each hand-off uses one coherent capture of the current admitted
  source epoch and driver generation of each asset, with the public-exposure
  and lifecycle gates of the
  [shared SemReg snapshot read](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/semantic-snapshot-read.md)
  already applied.
- An asset withheld by the capture
  (`semantic.read.reason.publication_withheld`, `asset_unknown`,
  `snapshot_unavailable`, `source_unavailable`) is handed over withheld with
  its reason verbatim; the gateway does not strip records, re-read a native
  source or fall back to cached state.
- A hand-off never mixes two captures, and partial failure of one asset does
  not replace another asset's last accepted result.

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

### Unsupported behavior

| Behavior | Gateway |
| --- | --- |
| Serving any eeBUS feature, use case or entity | Not in the gateway; the eeBUS registry serves it. |
| Mapping rows, target identity or target document | Not in the gateway. |
| Operation relay | Peer writes reach the semantic operation route through `internal/eebusoutput`; without an admitted route the relay is unwired. |
| New MCP tool, GraphQL field or namespace | None. Existing `eebus.v1.*` and `ebus.v1.*` surfaces are unchanged. |
| Authorization, trust or pairing change | None. |

The gateway side may not claim INT-12 normative completion, conformance,
certification, live-peer interoperability or hardware acceptance.

## Acceptance for the gateway side

- `internal/eebusoutput` exports no error or reason that refuses a mapping and
  no placeholder target identity or document.
- Offline synthetic tests cover the relay's unchanged results and errors, the
  unwired refusal and concurrent use.
