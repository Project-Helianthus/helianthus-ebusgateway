# AGENTS

These instructions apply to the entire repository.

## Workflow

1. Work one issue at a time and keep changes scoped to that issue.
2. Keep at most one open PR for this repository at any time.
3. Run `./scripts/ci_local.sh` before pushing.
4. React (emoji) to every review comment and reply with status when actioned.
5. If a change modifies externally visible behavior (transport options, GraphQL/MCP surface, smoke behavior), open/update the corresponding docs in `helianthus-docs-ebus` and merge docs alongside code (doc-gate).
6. Transport/protocol changes require a full 88-case runtime matrix pass (`TRANSPORT_MATRIX_REPORT=<index.json>`), unless explicitly overridden by owner approval (`TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` with a reason).
