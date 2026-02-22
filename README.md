# helianthus-ebusgateway

`helianthus-ebusgateway` is the runtime/API edge for Helianthus eBUS systems. It connects to an eBUS transport and exposes GraphQL, subscriptions, MCP, optional UI, and optional mDNS advertisement.

## Purpose and Scope

### What belongs in this repository

- Gateway runtime assembly (`gateway.go`, `cmd/gateway`).
- GraphQL query/mutation/subscription surfaces (`graphql/`).
- MCP JSON-RPC tool surface (`mcp/`).
- Optional UI mount and mDNS advertisement (`ui/`, `mdns/`).
- Hardware-backed smoke entrypoint and unknown-device dump plumbing (`cmd/smoke`, `smoke*.go`, `register_dump*.go`).

### What does not belong in this repository

- Low-level transport framing and bus primitives (use `helianthus-ebusgo`).
- Registry/provider model definitions and plane/projection semantics (use `helianthus-ebusreg`).
- Platform deployment bundles or auth/TLS edge policy management (handled by deployment infrastructure).

## Status and Maturity

- Active, CI-validated gateway service with race-enabled tests.
- Suitable for onboarding and issue-focused runtime/API changes.
- Smoke mode is intentionally opt-in and environment-backed (`EBUS_SMOKE=1` + local config file).

## Helianthus Dependency Chain

```text
helianthus-ebusgo  ->  helianthus-ebusreg  ->  helianthus-ebusgateway  ->  operators/automation clients
 (transport/proto)     (registry/schema)        (runtime/API)
```

## Quickstart (copy/paste)

### 0) Prerequisite: private module access (outside CI)

```bash
# Align local module settings with CI for private dependencies.
export GOPRIVATE='github.com/d3vi1/*'
export GONOSUMDB='github.com/d3vi1/*'
export GOPROXY=direct

# Use a GitHub token with read access to private repos.
export GH_TOKEN='<your_github_token>'

# CI uses a tokenized Git URL rewrite; keep local onboarding non-persistent.
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0="url.https://x-access-token:${GH_TOKEN}@github.com/.insteadOf"
export GIT_CONFIG_VALUE_0="https://github.com/"
```

After local checks, clear auth-related shell variables:
`unset GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0 GH_TOKEN`

### 1) Clone and baseline validation

```bash
git clone https://github.com/d3vi1/helianthus-ebusgateway.git
cd helianthus-ebusgateway
./scripts/ci_local.sh
go test ./...
go vet ./...
go build ./...
go test -race -count=1 ./...
```

### 2) Inspect runtime flags locally

```bash
go run ./cmd/gateway -h
```

### 3) Run gateway against a local ENH endpoint

```bash
go run ./cmd/gateway \
  -transport enh \
  -network unix \
  -address /var/run/ebusd/ebusd.socket \
  -http-addr :8080
```

### 4) Probe GraphQL and MCP surfaces

```bash
curl -fsS http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  --data '{"query":"{ __typename }"}'

curl -fsS http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":"ready","method":"ping","params":{}}'
```

## Local Smoke-Test Configuration Example

Smoke mode reads YAML blocks from repo-root `AGENT-local.md`. Minimal example:

```yaml
enh:
  type: unix
  path: /var/run/ebusd/ebusd.socket
  timeout_sec: 10

expected_devices:
  - address: 0x08
    description: "boiler"
    manufacturer: "Vaillant"
    device_id: "BAI00"
    sw_version: ""
    hw_version: ""

smoke:
  profile: enh
  source_address: 0x10
  scan_timeout_sec: 5
  method_timeout_sec: 10
  report_json_output: artifacts/smoke-report.json
```

Run smoke:

```bash
EBUS_SMOKE=1 go run ./cmd/smoke
```

Notes:
- `cmd/smoke` fails fast when `AGENT-local.md` is missing or invalid.
- Smoke checks are read-only and write a JSON report (`artifacts/smoke-report.json` by default).

## Transport Endpoint Examples

Protocol can be inferred from endpoint URI in `-address`:

```bash
go run ./cmd/gateway -address enh://127.0.0.1:19001 -http-addr :8080
go run ./cmd/gateway -address ens://127.0.0.1:19002 -http-addr :8080
go run ./cmd/gateway -address ebusd-tcp://127.0.0.1:9999 -http-addr :8080
go run ./cmd/gateway -address udp-plain://203.0.113.10:9999 -http-addr :8080
go run ./cmd/gateway -address tcp-plain://203.0.113.10:9999 -http-addr :8080
```

## Gateway Flag Cheat Sheet

| Flag | Default | Notes |
|---|---|---|
| `-transport` | `enh` | `enh`, `ens`, `ebusd-tcp`, `udp-plain`, `tcp-plain` |
| `-network` | `unix` | `unix`, `tcp`, or `udp` |
| `-address` | `/var/run/ebusd/ebusd.socket` | socket path, `host:port`, or endpoint URI |
| `-http-addr` | `:8080` | empty disables HTTP server |
| `-graphql-path` | `/graphql` | query/mutation endpoint |
| `-subscription-path` | `/graphql/subscriptions` | WebSocket/SSE subscriptions |
| `-snapshot-path` | `/snapshot` | projection snapshot endpoint |
| `-mcp-path` | `/mcp` | MCP JSON-RPC endpoint |
| `-ui-path` | `/ui` | set empty to disable UI |
| `-mdns` | `true` | set `false` outside trusted LAN |
| `-dump-upload-path` | _disabled_ | unknown-device dump upload endpoint path |

## Validation Commands

| Area | Command |
|---|---|
| terminology gate (CI parity) | `if git grep -nIwiE 'm[a]ster|s[l]ave'; then echo "Found legacy terminology."; exit 1; fi` |
| compile | `go build ./...` |
| vet | `go vet ./...` |
| tests (CI parity) | `go test -race -count=1 ./...` |
| smoke tests (unit/integration) | `go test ./... -run Smoke -count=1` |
| gateway flags smoke-check | `go run ./cmd/gateway -h` |
| smoke entrypoint smoke-check | `EBUS_SMOKE=0 go run ./cmd/smoke` |
| ebusd helper smoke-check | `go run ./cmd/ebusdscan -h` |

## Link Map

### Repositories and docs

- `helianthus-ebusgo`: https://github.com/d3vi1/helianthus-ebusgo
- `helianthus-ebusreg`: https://github.com/d3vi1/helianthus-ebusreg
- eBUS docs hub: https://github.com/d3vi1/helianthus-docs-ebus
- Smoke-test documentation: https://github.com/d3vi1/helianthus-docs-ebus/blob/main/development/smoke-test.md
- Issue tracker: https://github.com/d3vi1/helianthus-ebusgateway/issues

### Issue workflow conventions

- Use one issue-focused branch (example: `issue-93-readme-refresh`).
- Keep PR scope aligned to issue acceptance criteria.
- Include closing keyword in PR body (example: `Fixes #93`).
- Request agent review comment after opening the PR (`@codex review`).
