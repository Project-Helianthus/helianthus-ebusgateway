# Issue #264 author checkpoint: `ebus.v1.rpc.invoke` discoverability

## Scope and base

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/264
- Exact base: `25b96a0593357ff63de8315b803ec1262479c3df`
- Branch: `issue/264-rpc-invoke-discoverability`
- Author correction state: amended commit pushed to the issue branch; no GitHub
  API mutation, device action, or deployment. The Delivery Lead owns review and
  merge evidence for the complete candidate.

## Changed contract

`ebus.v1.rpc.invoke` now tells callers to discover the device, plane, and
method in that order through the registry list tools. Its `plane`, `method`,
and `params` schema properties identify the canonical input names and explain
that an omitted `params.source` uses the startup-admitted source. An explicitly
provided byte-validated nonzero source is a caller override of that startup
value; it is not compared against the admitted source.

This descriptor correction changes no invocation or source behavior. The
published-schema assertion and existing explicit-override guard tests pin the
wording to the implementation.

The invoke safety boundary is unchanged: explicit `intent` and
`allow_dangerous` remain required; `READ_ONLY` remains limited to a known
read-only method; `MUTATE` still requires dangerous-operation acknowledgement
and a non-empty idempotency key. Source normalization still precedes
idempotency-signature calculation. No method authority, transport behavior, or
device operation was added.

Validation failures for malformed top-level `address`, `plane`, `method`,
`intent`, and `allow_dangerous` now deterministically name the parameter and
expected shape. The repository already has an internal `ParamSchema` interface
used by GraphQL; it has no stable MCP serialization contract, so this change
does not add a second `params_schema` representation.

Unsupported non-empty `intent` values are rejected before registry, device, or
plane lookup. The public stale-route `DELETE` fixture pins that precedence and
its complete error envelope.

Whitespace is used only to reject an empty `plane` or `method`; authorization
and execution retain the original exact string. A padded name therefore fails
the authorization lookup instead of passing authorization and failing later at
execution with different behavior.

## Files

The final candidate changes 22 tracked files: seven implementation,
documentation, report, and characterization paths plus fifteen MCP goldens.
The public operation inventory also states the explicit nonzero RPC source
override rule, matching the schema and guard tests.

- `mcp/server_tools.go`: invoke discovery and parameter descriptions.
- `mcp/server.go`: parameter-specific invoke validation messages.
- `mcp/server_test.go`: RED-to-GREEN coverage for descriptions and invalid
  parameter messages.
- `mcp/parity_contract_test.go`,
  `mcp/server_construction_characterization_test.go`, and
  `mcp/testdata/growatt_protocol_ii_tools_list.golden.json`: intentional stable
  tools-list contract snapshots.
- `docs/architecture/runtime-driver-provider-contract-v1.md`: current gateway
  discovery/source-admission contract.

## Validation

RED first:

```text
GOWORK=off go test ./mcp -run 'TestServer_(ToolsList|InvokeV1SafetyErrorsNameInvalidParameter)' -count=1
FAIL: invalid address: ebus: payload does not match expected schema
```

GREEN focused checks:

```text
GOWORK=off go test ./mcp -run 'TestServer_(InitializeAndTools|InvokeV1SafetyErrorsNameInvalidParameter)' -count=1
PASS

GOWORK=off go test ./mcp -run TestParityMatrixReadAndInvoke -count=1
PASS

GOWORK=off go test ./mcp -count=1
PASS

GOWORK=off go test -race ./mcp -count=1
PASS
```

The public `tools/call` regression verifies that a malformed
`allow_dangerous` value produces a content-level stable envelope with
`INVALID_ARGUMENT` and the parameter-specific message.
Its checked-in golden captures the complete malformed-invoke result: `isError`,
envelope metadata, null data, and structured error code, source layer, and
message.

Additional normalized public-envelope goldens cover invalid addresses for each
affected registry list/get route and `rpc.invoke`, plus invalid `rpc.invoke`
plane and method values. They cover every intentional changed diagnostic while
normalizing only the live timestamp.

Complete repository gate, run from the finalized tree:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

The CI run completed terminology and source-selection gates, gofmt, portal
asset check, Node tests (93 passed, 0 failed), `go vet`, native and Linux
cross-builds, race tests, canonical PV shadow tests, source-selection artifact
coverage, Python script tests, transport gate, and passive smoke gate.

## Gates and residual risk

- Documentation gate: satisfied by the repository-owned runtime contract update.
- Transport gate: passed by `ci_local.sh`; no transport/protocol framing code
  changed.
- Runtime/smoke gate: passed the deterministic passive smoke gate; no live
  hardware or device smoke was requested or performed.
- Review: not requested at this author checkpoint.

Residual risk is limited to client presentation differences: MCP clients may
render JSON Schema `description` fields differently. The executable schema,
source-admission behavior, operation classification, and native invocation path
remain unchanged.

## Author return boundary

The specialist stopped after the requested amended commit and force-with-lease
push. The Delivery Lead owns feedback resolution and fresh review. No live
action is part of this issue.
