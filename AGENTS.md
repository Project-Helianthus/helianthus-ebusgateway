# AGENTS

These instructions apply to the entire repository.

## Workflow

1. Work one issue at a time and keep changes scoped to that issue.
2. Keep at most one open PR for this repository at any time.
3. Run `./scripts/ci_local.sh` before pushing.
4. React (emoji) to every review comment and reply with status when actioned.
5. If a change modifies externally visible behavior (transport options, GraphQL/MCP surface, smoke behavior), open/update the corresponding docs in `helianthus-docs-ebus` and merge docs alongside code (doc-gate).
6. Transport/protocol changes require a full 88-case runtime matrix pass (`TRANSPORT_MATRIX_REPORT=<index.json>`), unless explicitly overridden by owner approval (`TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` with a reason).

## MCP-first Policy

### Scope and ordering
- MCP is the primary prototyping/exploration interface.
- GraphQL is second and may reach parity only after MCP tools are deterministic and contract-solid.
- Home Assistant and other consumers are enabled only after GraphQL parity and stability gates are met.

### Tool taxonomy and naming
- Core stable tools use versioned names: `ebus.v<MAJOR>.<domain>.<subdomain>.<verb>`.
- Experimental tools live under `ebus.experimental.*` and are never used by external consumers.
- Prefer composable tools over monolithic endpoints.

### Contract envelope (required for ebus.v1.*)
Each `ebus.v1.*` tool returns:
- `meta` with `contract`, `consistency`, `data_timestamp`, `data_hash`
- `data`
- `error` (null or structured error)

### Determinism requirements
- List ordering must be stable.
- Snapshot mode must produce stable `data_hash` for identical snapshot + request.
- Tool schemas and outputs must have golden snapshots.

### Invoke safety
`ebus.v1.rpc.invoke` requires:
- explicit `intent` (`READ_ONLY` or `MUTATE`)
- `allow_dangerous=true` for mutating or unknown methods
- `idempotency_key` for mutating intent

### Graduation gates (MCP -> GraphQL)
A capability may graduate to GraphQL only if:
1. it exists as core stable MCP (`ebus.v1.*`)
2. it passes determinism + contract + golden tests
3. parity tests MCP <-> GraphQL are green

### End-of-cycle cleanup
At cycle end, each `ebus.experimental.*` tool must be promoted, deleted, or moved to internal-only with written justification.
No temporary/junk tool may remain in the showroom surface.

### CI gates
- Breaking changes in `ebus.v1.*` require a new major namespace.
- Parity drift MCP vs GraphQL fails CI.
