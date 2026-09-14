# Gateway #975 B503 source-contract parity

Reviewed Gateway source: `a39d43fbeaf8d745222b85649ebb8494203163f0`.

Reviewed documentation contract: `helianthus-docs-ebus` commit
`07373de4ad8063dd40a0b0cff4692df052b0fa87`,
`api/portal.md`, INT-10 target section.

## Result

One reachable parity gap was found and corrected locally. The raw-frame
dispatcher already distinguishes a selected-target cancellation before bus
turnaround from a NAK, CRC, or other protocol failure. The GraphQL provider
previously returned those errors unchanged, so the browser-visible GraphQL
error could contain only the internal dispatcher wording rather than the
contractual public code.

`cmd/gateway/vaillant_b503_graphql_provider.go` now prefixes only those two
dispatcher classifications with `UPSTREAM_TIMEOUT` and
`UPSTREAM_RPC_FAILED`. It leaves transport, session, validation, and unrelated
errors owned by their existing paths. The new GraphQL regression executes both
sentinels through the `vaillantErrors` root and proves the field-local response
contains the required public code.

## Runtime/FSM comparison

| Contract behavior | Reachable implementation and test evidence | Verdict |
|---|---|---|
| Target/nav changes invalidate old presentation before a new request and preserve an old token-target cleanup pair. | `portal/web/src/app.js` lines 3663-3736 publishes `PENDING`, advances the presentation epoch, starts detached cleanup, and fences the later probe. Browser tests cover stale enable, delayed cleanup, rapid target changes, and no blocked qualification. | Pass |
| A `Refreshing` owner exposes only status while capability is `UNKNOWN`; no new B503 operation is rendered. | Lines 3774-3820 retain the strip only and omit the tab/action markup outside `AVAILABLE`; `VaillantB503Pane_refreshingStripRemainsVisibleWithoutOperationsWhenCapabilityUnknown` passes. | Pass |
| Refresh holds the gate; a later successful refresh continues the same owner and deferred cleanup is bounded/token-bound. | Lines 4278-4311 serialize bounded status reads and disable controls while `Enabling` or `Refreshing`; lines 4318-4416 retain and reconcile only the captured pair. Focused tests cover authoritative Refreshing, settled Active retry, finite status bounds, and disconnect fences. | Pass |
| A disconnect releases ownership; reconnect is not automatic recovery and stale epoch replies cannot publish. | `cmd/gateway/vaillant_b503_dispatcher.go` lines 200-262 discard stale epoch completions. The M6 truth-table tests cover post-reconnect `UNKNOWN` before a successful probe, then `AVAILABLE`; browser coverage proves reconnect cannot turn stale status work into a cleanup write. | Pass |
| Field cancellation maps to `UPSTREAM_TIMEOUT`; NAK/CRC/arbitration maps to `UPSTREAM_RPC_FAILED`; neither replaces the last-known capability with `TRANSPORT_DOWN`. | Dispatcher classification at lines 275-328 already makes this distinction. This review added `publicB503GraphQLError` at the GraphQL provider boundary and `TestIssue552B503GraphQLPreservesDispatcherErrorCodes`. | Corrected |

## Validation

- `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 54 tests.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./cmd/gateway ./mcp ./graphql -run 'Test(Issue552B503GraphQLPreservesDispatcherErrorCodes|VaillantB503|M6|Issue552)' -count=1`: PASS.

## Scope and remaining gate

This is an offline source-contract comparison. It performs no live operation,
does not change Portal routing or lifecycle ownership, and is not an
exact-HEAD review or merge verdict. Full repository CI, current feedback
reconciliation, and fresh independent review remain downstream gates.
