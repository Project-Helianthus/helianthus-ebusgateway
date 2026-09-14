# Gateway #975 B503 source-contract parity

Reviewed Gateway baseline: `6ba3777ecc1ed88979c0156c3f2c995c70b9a87c`,
plus the conservative settlement correction described below.

Reviewed documentation contract: `helianthus-docs-ebus` commit
`07373de4ad8063dd40a0b0cff4692df052b0fa87`,
`api/portal.md`, INT-10 target section; and B503 functional contract commit
`867752765469c1cbb110d439dac362de81abed4c`.

## Result

The public GraphQL error-code parity gap and Manager lifecycle gaps remain
closed. This correction removes one unsafe settlement assumption: eBUS native
ACK and NAK are retained as exact wire outcomes, but neither is treated as
proof that the device-side live-monitor session settled.

`b503session.Manager` owns the complete target-bound enable/read/disable
operation, native outcome, transport epoch, process-local cleanup obligation,
and restart fence. Every enable that enters `bus.Send` creates that cleanup
obligation. An exact enable ACK may establish the current process owner, but it
does not remove cleanup or publish capability `AVAILABLE`. An emitted NAK or
other failure releases caller ownership while retaining cleanup and
`UNKNOWN`. Disable and later-epoch cleanup ACK/NAK outcomes are recorded and
returned exactly; they do not clear cleanup, permit re-Enable, or auto-recover
the target.

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
| ACTIVE disconnect returns `TRANSPORT_DOWN` only to the in-flight request, while availability remains `UNKNOWN` under retained cleanup. | `OnTransportDisconnect` releases only a held owner and retains target, fresh Gateway cleanup ID, and prior epoch. Corrected row-3 coverage proves the in-flight error, released ownership, retained target, and exact `UNKNOWN` capability. | Pass |
| Restart destroys caller handles but fences each qualified target at `UNKNOWN`, with no Enable until authorized recovery. | `ResetForRestart` clears caller and process-local cleanup tuples, records qualified-target restart fences, and never invokes the cleanup dispatcher. Operation coverage proves later epoch advance emits no write and public Enable stays unavailable. | Pass |
| Enable stays `Enabling` until its exact native outcome; every emitted outcome retains cleanup, while pre-emission cancellation returns Idle without cleanup. | `EnableOperation` creates cleanup for emitted ACK, NAK, and ambiguous outcomes. ACK may install the current owner without claiming settlement; NAK/error releases it. Deterministic tests cover pending state, zero-write pre-emission cancellation, exact ACK/NAK evidence, retained cleanup, and denied re-Enable. | Pass |
| Explicit disable emits one native disable, releases caller ownership, and remains fail-closed because ACK/NAK does not prove settlement. | MCP and GraphQL call `DisableOperation`, which records the attempt before dispatch and retains its exact outcome in internal cleanup evidence. Tests prove ACK, NAK, and timeout cannot clear cleanup or re-admit Enable. | Pass |
| Reconnect makes one bounded cleanup attempt per later epoch for a retained obligation, never reconstructs an owner, and never treats ACK/NAK as recovery. | `OnEpochAdvance` records each exact cleanup outcome and preserves the same target/attempt fence. Tests assert dispatch target/count, stable cleanup ID, no same-epoch retry, later-epoch ACK retention, and the restart no-write fence. | Pass |
| Field cancellation maps to `UPSTREAM_TIMEOUT`; NAK/CRC/arbitration maps to `UPSTREAM_RPC_FAILED`; neither replaces the last-known capability with `TRANSPORT_DOWN`. | Dispatcher classification at lines 275-328 already makes this distinction. This review added `publicB503GraphQLError` at the GraphQL provider boundary and `TestIssue552B503GraphQLPreservesDispatcherErrorCodes`. | Corrected |

## Validation

- `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 54/54.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./internal/vaillant/b503session ./cmd/gateway ./mcp ./graphql -run 'Test(Operation|Session_|Issue552B503|Issue552VaillantB503|M6TruthTable|M6Conc|M6Dispatcher|VaillantB503|Issue851B503)' -count=1 -timeout=120s`: PASS, 95/95 selected tests (26 Manager, 37 Gateway, 21 MCP, 11 GraphQL).
- `rg` over non-test Go files finds no legacy `Manager.Enable`, `Manager.Read`, or `Manager.Disable` production call site; MCP and GraphQL use only the operation-owned API.

## API/composition result

The correction keeps the Manager-owned design. `DispatchOutcome` carries
response, emitted boundary, native result, exact error, and issuing transport.
`CleanupObligationSnapshot` retains target, fresh Gateway cleanup attempt ID,
origin outcome, later cleanup outcome, epoch evidence, and no issuer token.
The process-local hook is installed once by Gateway wiring and is used for idle
and defensive cleanup. Lifecycle calls fail closed before emission when that
outcome-aware hook is absent; there is no generic dispatcher fallback. Restart
clears the process-local tuple, creates per-qualified-target fences, and invokes
no write.

## Scope and remaining gate

This is an offline implementation and source-contract comparison. It performs
no live operation, authorized recovery, persistence, push, review, or merge.
Portal routing is unchanged; Gateway lifecycle ownership is corrected. Full
repository CI, current feedback reconciliation, and fresh independent
exact-HEAD review remain downstream gates. The Board-approved conservative
settlement correction supersedes documentation rows that treat native ACK or
NAK as settlement proof; reconciling those rows is outside this Gateway-only
write set.
