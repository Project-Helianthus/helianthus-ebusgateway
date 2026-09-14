# Gateway #975 B503 source-contract parity

Reviewed Gateway baseline: `beae3f8587a53060296e9e218282ba7104b2b0e5`,
plus the uncommitted lifecycle correction described below.

Reviewed documentation contract: `helianthus-docs-ebus` commit
`07373de4ad8063dd40a0b0cff4692df052b0fa87`,
`api/portal.md`, INT-10 target section; and B503 functional contract commit
`867752765469c1cbb110d439dac362de81abed4c`.

## Result

The public GraphQL error-code parity gap and the five Manager lifecycle gaps
below are corrected in the working tree. `b503session.Manager` now owns the
complete target-bound enable/read/disable operation, native outcome, transport
epoch, process-local cleanup obligation, and restart fence. MCP and GraphQL
share that Manager and no longer mutate ownership before or after a separate
dispatcher call.

The generic `mcp.RPCDispatcher` interface is unchanged. Its optional
`B503OutcomeDispatcher` extension reports `Emitted=false` only before
`bus.Send` and reports emitted outcomes conservatively thereafter. It preserves
native ACK, native NAK, and ambiguous outcomes so Manager can apply the
documented cleanup rules without inferring wire state from a public error.

## Runtime/FSM comparison

The first three rows are Portal behavior. The remaining rows are implemented
by the shared Gateway Manager and production dispatcher path.

| Contract behavior | Reachable implementation and test evidence | Verdict |
|---|---|---|
| Target/nav changes invalidate old presentation before a new request and preserve an old token-target cleanup pair. | `portal/web/src/app.js` lines 3663-3736 publishes `PENDING`, advances the presentation epoch, starts detached cleanup, and fences the later probe. Browser tests cover stale enable, delayed cleanup, rapid target changes, and no blocked qualification. | Pass |
| A `Refreshing` owner exposes only status while capability is `UNKNOWN`; no new B503 operation is rendered. | Lines 3774-3820 retain the strip only and omit the tab/action markup outside `AVAILABLE`; `VaillantB503Pane_refreshingStripRemainsVisibleWithoutOperationsWhenCapabilityUnknown` passes. | Pass |
| Refresh holds the gate; a later successful refresh continues the same owner and deferred cleanup is bounded/token-bound. | Lines 4278-4311 serialize bounded status reads and disable controls while `Enabling` or `Refreshing`; lines 4318-4416 retain and reconcile only the captured pair. Focused tests cover authoritative Refreshing, settled Active retry, finite status bounds, and disconnect fences. | Pass |
| ACTIVE disconnect returns `TRANSPORT_DOWN` only to the in-flight request, while availability is `UNKNOWN` until defensive cleanup succeeds. | `OnTransportDisconnect` releases only a held owner and retains target, fresh Gateway cleanup ID, and prior epoch. Corrected row-3 coverage proves the in-flight error, released ownership, retained target, and exact `UNKNOWN` capability. | Pass |
| Restart destroys caller handles but fences each qualified target at `UNKNOWN`, with no Enable until authorized recovery. | `ResetForRestart` clears caller and cleanup tuples, records qualified-target restart fences, and never invokes the cleanup dispatcher. Operation coverage proves later epoch advance emits no write and public Enable stays unavailable. | Pass |
| Enable becomes `Active` only after enable ACK; ambiguous post-emission failure retains cleanup while a pre-emission cancellation returns Idle. | `EnableOperation` holds `Enabling` until native ACK. Deterministic tests cover the visible pending state, pre-emission cancellation with zero cleanup writes, enable NAK with no cleanup, and ambiguous emission followed by exactly one target-bound cleanup. | Pass |
| Explicit disable emits one native disable and enters `Idle` only after valid ACK; failed cleanup remains fail-closed. | MCP and GraphQL call `DisableOperation`, which releases caller ownership into internal `Disabled`, records the attempt before dispatch, and clears it only for native ACK. Tests cover ACK and NAK/timeout retention. | Pass |
| Reconnect makes one bounded cleanup attempt only for a retained obligation, never reconstructs an owner or writes automatically after restart. | `OnEpochAdvance` attempts retained cleanup once per later epoch and reuses neither caller authority nor a stale completion. Tests assert dispatch target/count, stable cleanup ID, no same-epoch retry, later-epoch ACK clearing, and the restart no-write fence. | Pass |
| Field cancellation maps to `UPSTREAM_TIMEOUT`; NAK/CRC/arbitration maps to `UPSTREAM_RPC_FAILED`; neither replaces the last-known capability with `TRANSPORT_DOWN`. | Dispatcher classification at lines 275-328 already makes this distinction. This review added `publicB503GraphQLError` at the GraphQL provider boundary and `TestIssue552B503GraphQLPreservesDispatcherErrorCodes`. | Corrected |

## Validation

- `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 54/54.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./internal/vaillant/b503session ./cmd/gateway ./mcp ./graphql -run 'Test(Operation|Session_|Issue552B503|Issue552VaillantB503|M6TruthTable|M6Conc|M6Dispatcher|VaillantB503)' -count=1 -timeout=120s`: PASS, 85/85 selected tests (24 Manager, 30 Gateway, 20 MCP, 11 GraphQL).
- `rg` over non-test Go files finds no legacy `Manager.Enable`, `Manager.Read`, or `Manager.Disable` production call site; MCP and GraphQL use only the operation-owned API.

## API/composition result

The correction implements the Manager-owned option. `DispatchOutcome` carries
response, emitted boundary, native result, exact error, and issuing transport.
`CleanupObligationSnapshot` carries target, fresh Gateway cleanup attempt ID,
origin/later epoch evidence, and no issuer token. The process-local hook is
installed once by Gateway wiring and is used for idle and defensive cleanup;
restart clears the hook-owned tuple without invoking it.

## Scope and remaining gate

This is an offline implementation and source-contract comparison. It performs
no live operation, authorized recovery, persistence, push, review, or merge.
Portal routing is unchanged; Gateway lifecycle ownership is corrected. Full
repository CI, current feedback reconciliation, and fresh independent
exact-HEAD review remain downstream gates.
