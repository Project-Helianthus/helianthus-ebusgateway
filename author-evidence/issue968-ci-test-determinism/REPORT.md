# Gateway #968 CI determinism author report

Source `e0968897903f5b944986409e61ef54e426ad8c29`, tree `b06c52148dff08461ba479f4404ae89e87f156c4`.

Only test fixtures changed. The MCP partial-snapshot test uses a request context cancelled when `Zones()` enters after `DHW()` completes, exercising existing post-read cancellation deterministically. The SSE test shares request context with delivery completion instead of an unrelated two-second timer. Production source is unchanged.

- Focused `GOWORK=off go test -race ./mcp ./graphql -run 'TestServer_ToolsCallSemanticSnapshots/semantic_snapshot_timeout_partial$|TestBroadcastSubscriptions_Integration/sse$' -count=10`: PASS; SHA-256 `c9399de86e493945eb19b6959dd84b6252de525ad411c14cd1b657ebc79803f0`.
- Complete `GOWORK=off ./scripts/ci_local.sh`: PASS; SHA-256 `3e0fdd6b1fbe6f1a657995dbaa0bb1123a1163a9a5cb37af5da1fd7f22bb2fce`.

Docs gate not required; transport and smoke gates not triggered. No live action.

## Third hosted race correction

Source `b9a6371` adds a transport-owned write-return signal immediately before the managed provider `Write` returns. The BACKOFF callback therefore proves it cannot cross the admitted provider call without depending on caller-side `doSend` scheduling. Alongside the deterministic MCP request-context partial fixture and request-context SSE delivery fixture, all three tests pass ten repeated race runs.

- Three-fixture focused race SHA-256: `97c5b94541e31744816c2d3f125de0406b4b6db5f5918bf575d3e`.
- Complete CI SHA-256: `3808b28d213936f9af2059e4802ad2040567dc0deb185655ed5fd120477583c5`.

## Deadline classification correction

Final implementation source `6ac0cc27ce127e08782d584cba866fa1466177e0`, tree `6f4488e8289d9d753526ecc7ef86c94e72607fd8`. The unexported per-Server timeout factory defaults to `context.WithTimeout`; the test supplies a synchronous manual deadline context. It verifies exactly `[dhw]` completed and one `zones` `TIMEOUT` error while retaining no-duplicate coverage.

- `GOWORK=off go test -race ./internal/adaptermux ./mcp ./graphql -run 'TestManagedConnectionLossLinearizesProxyAdmissionAndProviderUse/blocked_write_drains_before_BACKOFF_publication$|TestServer_ToolsCallSemanticSnapshots/semantic_snapshot_timeout_partial$|TestServer_ToolsCallSemanticSnapshots/semantic_snapshot_timeout_partial_no_duplicate_plane_errors$|TestBroadcastSubscriptions_Integration/sse$' -count=10` — PASS; SHA-256 `1e2d62e73438c7725def9b7676b51f0002b415354321456e35026581ffe4d36d`.
- `GOWORK=off ./scripts/ci_local.sh` — PASS; SHA-256 `8111791bac5a610478b1405a50fd1122350d34c0c0eb9f2e96c9a72cd63ae3e0`.
