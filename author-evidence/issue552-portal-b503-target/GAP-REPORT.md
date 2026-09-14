# Gateway #975 B503 source-contract parity

Reviewed Gateway source parent: `c9045ac9fa4ddeb1c8462662ace2314f3e1f39c7`,
plus the emission-boundary correction described below.

Compared accepted documentation contract: `helianthus-docs-ebus` PR #524 merge
`7a9a9d0501161a0b7a56522c3d4afa0677abcefd`, tree
`05f8de4ffb3d09262fe4d8b822499fc1a6330d55`. The independently reviewed
candidate `bc5cb66f7542e0618e03c70a82875d9ed641f813` has that same tree.

## Result

The public GraphQL error-code and Portal presentation gaps remain closed. At
the runtime boundary, a terminal disconnect while an Enable or current-owner
Disable is still waiting for poll quiescence now invalidates the exact pending
operation before `bus.Send`. A queued Enable returns exact `TRANSPORT_DOWN` to
ownerless `IDLE`, clears its attempt, and creates no cleanup. A queued Disable
returns exact `TRANSPORT_DOWN`, releases its owner, and retains the existing
target/attempt/epoch cleanup evidence. The refreshed Disable case retains the
same obligation at the successfully rebound epoch. Reconnect never drains or
retries either queued frame.

When capability is `UNKNOWN`, only the same browser holding the exact selected
target's current token may retain an `Active`/`owned:true` session strip and
usable READ/DISABLE controls. It does not regain Enable, general B503 tabs, or
a projection entry. Every other `UNKNOWN` target or non-owner presentation
remains operation-free.

`b503session.Manager` owns the complete target-bound enable/read/disable
operation, native outcome, transport epoch, process-local cleanup obligation,
and restart fence. Every enable that enters `bus.Send` creates that cleanup
obligation. An exact enable ACK may establish the current process owner, but it
does not remove cleanup or publish capability `AVAILABLE`. An emitted NAK or
other failure releases caller ownership while retaining cleanup and
`UNKNOWN`. Disable ACK/NAK outcomes are recorded and returned exactly; they do
not clear cleanup, permit re-Enable, or auto-recover the target. Terminal
disconnect, reconnect, and restart emit no automatic recovery write while
settlement remains unproven.

If an emitted Enable crosses an epoch during its admitted lifecycle, Manager
fences the stale completion and invokes at most one target-bound defensive
disable after the dispatch quiesces. That attempt remains evidence rather than
settlement. Later reconnect epochs emit no automatic retry.

The generic `mcp.RPCDispatcher` interface is unchanged. Its optional
`B503OutcomeDispatcher` extension reports `Emitted=false` only before
`bus.Send` and reports emitted outcomes conservatively thereafter. It preserves
native ACK, native NAK, and ambiguous outcomes so Manager can apply the
documented cleanup rules without inferring wire state from a public error.

## Runtime/FSM comparison

The first four rows are Portal behavior. The remaining rows are implemented
by the shared Gateway Manager and production dispatcher path.

| Contract behavior | Reachable implementation and test evidence | Verdict |
|---|---|---|
| Target/nav changes invalidate old presentation before a new request and preserve an old token-target cleanup pair. | `_beginVaillantB503TargetQualification` publishes `PENDING`, advances the presentation epoch, starts detached cleanup, and fences the later probe. Browser tests cover stale enable, delayed cleanup, rapid target changes, and no blocked qualification. | Pass |
| A `Refreshing` owner exposes only status while capability is `UNKNOWN`; no new B503 operation is rendered. | `renderVaillantB503Pane` retains the strip only for `Refreshing`/`owned:true` on the live-monitor view and omits all action markup. `VaillantB503Pane_refreshingStripRemainsVisibleWithoutOperationsWhenCapabilityUnknown` covers the row. | Pass |
| The same browser's selected-target `Active` owner retains only READ/DISABLE while capability is `UNKNOWN`. | `renderVaillantB503Pane` requires live-monitor view, `Active`, `owned:true`, a non-empty local token, and an exact token-target match before rendering the bounded owner markup. Tests prove READ/DISABLE are bound, Enable/tabs/projection stay absent, other targets and non-owners inherit nothing, and ownership loss immediately unmounts the exception. | Corrected |
| Refresh holds the gate; a later successful refresh continues the same owner and deferred cleanup is bounded/token-bound. | Session-status requests remain serialized and disable controls stay unavailable during `Enabling` or `Refreshing`; deferred cleanup retains only the captured pair. Focused tests cover authoritative Refreshing, settled Active retry, finite status bounds, and disconnect fences. | Pass |
| ACTIVE disconnect returns `TRANSPORT_DOWN` only to the in-flight request, while availability remains `UNKNOWN` under retained cleanup. | `OnTransportDisconnect` releases only a held owner and retains target, fresh Gateway cleanup ID, and prior epoch. Corrected row-3 coverage proves the in-flight error, released ownership, retained target, and exact `UNKNOWN` capability. | Pass |
| Restart destroys caller handles but fences each qualified target at `UNKNOWN`, with no Enable until authorized recovery. | `ResetForRestart` clears caller and process-local cleanup tuples, records qualified-target restart fences, and never invokes the cleanup dispatcher. Operation coverage proves later epoch advance emits no write and public Enable stays unavailable. | Pass |
| Enable stays `Enabling` until its exact native outcome; every emitted outcome retains cleanup, while pre-emission cancellation returns Idle without cleanup. | `EnableOperation` creates cleanup for emitted ACK, NAK, and ambiguous outcomes. ACK may install the current owner without claiming settlement; NAK/error releases it. Deterministic tests cover pending state, zero-write pre-emission cancellation, exact ACK/NAK evidence, retained cleanup, and denied re-Enable. | Pass |
| ENABLING transport disconnect before frame emission returns exact `TRANSPORT_DOWN` to ownerless `IDLE`, clears the pending attempt, retains no cleanup, and requires a new explicit Enable after reconnect. | Manager puts its private operation identity in the dispatch context. After `readMu` quiesces, the production dispatcher admits that exact operation immediately before `bus.Send`; disconnect or epoch invalidation makes admission fail without I/O. `TestIssue552B503EnableDisconnectBeforeEmissionReturnsIdleWithoutCleanup` holds the real `readMu` boundary, proves zero sends and no cleanup, advances the epoch, and proves only a new explicit Enable can claim ownership. | Corrected |
| Epoch advance after enable-frame emission permits at most one defensive disable during that admitted lifecycle and no later automatic retry. | `EnableOperation` resolves the pending epoch only after the emitted outcome returns, creates one target-bound cleanup obligation, and calls its cleanup hook once. `TestOperationEnablePostEmissionEpochAdvanceRunsAtMostOneLifecycleCleanup` proves exact target/outcome/epoch evidence, released ownership, denied re-Enable, and zero writes on subsequent epochs. | Pass |
| Current-owner DISABLE disconnect before frame emission, including while waiting for poll quiescence, emits no disable and returns exact `TRANSPORT_DOWN`; Active retains the prior cleanup epoch and refreshed Disable retains the rebound epoch. | `DisableOperation` now holds ownership until the dispatch reaches its emission admission boundary. `TestIssue552B503DisableDisconnectBeforeEmissionRetainsFenceWithoutWrite` covers Active and refreshed paths at the real `readMu` boundary, exact error, zero sends, stable cleanup ID/target/epoch, owner release, repeated later epochs, and denied re-Enable. | Corrected |
| Explicit disable after frame emission emits once, releases caller ownership, and remains fail-closed because ACK/NAK does not prove settlement. | MCP and GraphQL call `DisableOperation`, which records the exact emitted or terminal outcome while retaining internal cleanup evidence. Tests prove ACK, NAK, and timeout cannot clear cleanup or re-admit Enable. | Pass |
| Post-emission ENABLING disconnect remains conservative and never becomes the pre-emission escape path. | The dispatcher marks the Manager operation emitted immediately before entering `bus.Send`. `TestIssue552B503EnableDisconnectAfterEmissionRetainsUnknownFence` disconnects while Send is in flight and proves one send, `OriginEmitted=true`, retained cleanup, ownerless public Idle, and zero later reconnect writes. | Corrected |
| Reconnect advances transport evidence but emits no automatic recovery write for a retained obligation, never reconstructs an owner, and keeps the target unavailable. | `OnEpochAdvance` updates the Manager transport epoch while preserving the exact cleanup snapshot and fence. Deterministic Manager and DriverManager lifecycle tests assert zero writes across repeated later epochs, stable cleanup evidence, `UNKNOWN`, denied re-Enable, and the restart no-write fence. | Pass |
| Every successful Active read resets the idle timer. | `ReadOperation` rearms the generation-guarded timer only after exact native ACK and a still-current owner/transport check. `TestOperationSuccessfulReadResetsIdleTimer` proves the production operation remains owned beyond the original deadline; non-ACK failures leave the timer untouched. | Pass |
| Field cancellation maps to `UPSTREAM_TIMEOUT`; NAK/CRC/arbitration maps to `UPSTREAM_RPC_FAILED`; neither replaces the last-known capability with `TRANSPORT_DOWN`. | `classifySendErr` preserves this distinction. The prior correction added `publicB503GraphQLError` at the GraphQL provider boundary and `TestIssue552B503GraphQLPreservesDispatcherErrorCodes`. | Pass |

## Validation

- `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 57/57.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./internal/vaillant/b503session ./cmd/gateway ./mcp ./graphql -run 'Test(Operation|Session_|Issue552B503|Issue552VaillantB503|M6TruthTable|M6Conc|M6Dispatcher|VaillantB503|Issue851B503)' -count=1 -timeout=120s`: PASS, 100/100 selected top-level tests (28 Manager, 40 Gateway, 21 MCP, 11 GraphQL). The Gateway total includes an Active/refreshed subtest pair at the post-quiesce emission boundary.
- `rg` over non-test Go files finds no legacy `Manager.Enable`, `Manager.Read`, or `Manager.Disable` production call site; MCP and GraphQL use only the operation-owned API.

## API/composition result

The correction keeps the Manager-owned design. `DispatchOutcome` carries
response, emitted boundary, native result, exact error, and issuing transport.
`CleanupObligationSnapshot` retains target, fresh Gateway cleanup attempt ID,
origin outcome, explicit cleanup outcome, epoch evidence, and no issuer token.
The Manager attaches an unforgeable package-private context key carrying only
the pending operation ID; the dispatcher consumes it to admit the same target
at the final pre-Send boundary. The process-local hook is installed once by
Gateway wiring and is used for idle expiry and same-operation ambiguous
cleanup. Lifecycle calls fail closed before emission when that outcome-aware
hook is absent; there is no generic dispatcher fallback. Restart clears the
process-local tuple, creates per-qualified-target fences, and invokes no write.

## Scope and remaining gate

This is an offline implementation and source-contract comparison. It performs
no live operation, authorized recovery, persistence, push, review, or merge.
Portal routing is unchanged; Gateway lifecycle ownership and the bounded
current-owner presentation match the accepted docs tree. Full repository CI,
current feedback reconciliation, and fresh independent exact-HEAD review remain
downstream gates.
