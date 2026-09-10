# Gateway #968 CI determinism author report

Source `e0968897903f5b944986409e61ef54e426ad8c29`, tree `b06c52148dff08461ba479f4404ae89e87f156c4`.

Only test fixtures changed. The MCP partial-snapshot test uses a request context cancelled when `Zones()` enters after `DHW()` completes, exercising existing post-read cancellation deterministically. The SSE test shares request context with delivery completion instead of an unrelated two-second timer. Production source is unchanged.

- Focused `GOWORK=off go test -race ./mcp ./graphql -run 'TestServer_ToolsCallSemanticSnapshots/semantic_snapshot_timeout_partial$|TestBroadcastSubscriptions_Integration/sse$' -count=10`: PASS; SHA-256 `c9399de86e493945eb19b6959dd84b6252de525ad411c14cd1b657ebc79803f0`.
- Complete `GOWORK=off ./scripts/ci_local.sh`: PASS; SHA-256 `3e0fdd6b1fbe6f1a657995dbaa0bb1123a1163a9a5cb37af5da1fd7f22bb2fce`.

Docs gate not required; transport and smoke gates not triggered. No live action.
