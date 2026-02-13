# helianthus-ebusgateway

Gateway service for eBUS systems. It connects to an eBUS transport (`ENH`, `ENS`, or `ebusd-tcp`) and exposes a stable API surface for operators and automation:

- GraphQL query/mutation/subscription endpoints
- MCP JSON-RPC endpoint for agent/tool integrations
- Optional mDNS service advertisement for GraphQL discovery
- Optional register-dump upload endpoint for unknown-device analysis

## Purpose and scope

`helianthus-ebusgateway` is the API edge for the Helianthus eBUS stack:

- Uses `helianthus-ebusgo` for transport + protocol bus I/O
- Uses `helianthus-ebusreg` for registry, planes, projections, and router semantics
- Serves GraphQL/MCP/UI over HTTP for external consumers

It is intended for engineers/operators who need a programmable interface to eBUS devices and projections. It is not a full deployment bundle (you still provide transport connectivity and, when needed, provider configuration).

## Runtime topology

```text
eBUS adapter/socket (unix|tcp)
        |
        v
helianthus-ebusgo transport/protocol bus
        |
        +--> helianthus-ebusreg DeviceRegistry
        +--> helianthus-ebusreg BusEventRouter
                   |
                   +--> GraphQL (/graphql, /graphql/subscriptions, /snapshot)
                   +--> MCP (/mcp)
                   +--> UI (/ui)
                   +--> mDNS advertisement (_helianthus-graphql._tcp)
```

## Prerequisites

- Go `1.22+`
- Reachable eBUS transport endpoint:
  - `unix` socket (commonly `/var/run/ebusd/ebusd.socket`), or
  - `tcp` host:port
- Access to private Helianthus modules if building outside CI (set auth + `GOPRIVATE=github.com/d3vi1/*` as needed)

## Quick start

From repo root:

```bash
go build ./...
go test ./...
```

Run gateway with default ENH-over-unix assumptions:

```bash
go run ./cmd/gateway \
  -transport enh \
  -network unix \
  -address /var/run/ebusd/ebusd.socket \
  -http-addr :8080
```

Basic GraphQL probe:

```bash
curl -sS http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  --data '{"query":"{ devices { address manufacturer deviceId planes { name } } }"}'
```

## Deployment profiles (dev / HA / prod-like)

### Dev profile (single instance, local-first)

Use defaults and local transport assumptions:

```bash
go run ./cmd/gateway \
  -transport enh \
  -network unix \
  -address /var/run/ebusd/ebusd.socket \
  -http-addr :8080
```

Good for local validation with UI + GraphQL + MCP exposed on one node.

### HA profile (multi-instance API tier)

Run multiple identical `cmd/gateway` instances behind a reverse proxy/load balancer:

- Each instance keeps its own in-memory registry/router state.
- Use the same transport/provider wiring per instance.
- Prefer `-mdns=false` to avoid duplicate DNS-SD announcements unless explicitly needed.
- Probe each instance at API level (`/graphql`, `/mcp`) before admitting traffic.

Per-instance example:

```bash
go run ./cmd/gateway \
  -transport enh \
  -network tcp \
  -address 127.0.0.1:9999 \
  -http-addr :8080 \
  -mdns=false
```

### Production-like profile (single hardened edge)

Keep gateway private and terminate auth/TLS at the edge proxy:

```bash
go run ./cmd/gateway \
  -transport enh \
  -network unix \
  -address /var/run/ebusd/ebusd.socket \
  -http-addr 127.0.0.1:8080 \
  -mdns=false \
  -ui-path '' \
  -dump-upload-path ''
```

This keeps optional surfaces disabled by default while preserving GraphQL/MCP API access through the proxy.

## Security and hardening notes

- `cmd/gateway` serves plain HTTP only; there is no in-process TLS termination.
- API endpoints are unauthenticated in current implementation; enforce authN/authZ upstream (reverse proxy, API gateway, service mesh).
- Auth roadmap in current deployments is edge-enforced authentication/authorization; this binary has no auth flags today.
- Default `-http-addr :8080` binds on all interfaces. For hardened setups, bind loopback/private interfaces and publish only via proxy.
- `-mdns` defaults to `true`; disable outside trusted LAN segments to avoid service discovery leakage.
- Leave `-dump-upload-path` unset unless you intentionally need register-dump upload ingestion.
- `-dump-include-pii` defaults to `false`; keep it disabled in shared/production-like environments.
- GraphQL subscription WebSocket upgrade currently accepts all origins; enforce origin policy and allowed callers at the edge.

## Configuration cheat sheet (gateway command)

`cmd/gateway` is flag-driven (no environment-variable parsing in this command).

| Flag | Default | Notes |
| --- | --- | --- |
| `-transport` | `enh` | `enh`, `ens`, or `ebusd-tcp` |
| `-network` | `unix` | Transport dial network (`unix` or `tcp`) |
| `-address` | `/var/run/ebusd/ebusd.socket` | Transport socket path or `host:port` |
| `-read-timeout` | `5s` | Transport read timeout |
| `-write-timeout` | `5s` | Transport write timeout |
| `-dial-timeout` | `5s` | Transport dial timeout |
| `-queue-capacity` | `0` | `0` uses protocol default |
| `-broadcast` | `false` | Starts separate broadcast listener connection |
| `-http-addr` | `:8080` | Empty disables HTTP server |
| `-graphql-path` | `/graphql` | GraphQL query/mutation endpoint |
| `-snapshot-path` | `/snapshot` | Projection snapshot endpoint |
| `-subscription-path` | `/graphql/subscriptions` | WebSocket/SSE subscription endpoint |
| `-mcp-path` | `/mcp` | MCP JSON-RPC endpoint |
| `-ui-path` | `/ui` | Portal UI mount path (set empty to disable) |
| `-dump-upload-path` | _disabled_ | Register-dump upload endpoint path |
| `-dump-output-dir` | `./dumps` | Dump output directory |
| `-dump-upload-url` | _empty_ | Internal unknown-device dump upload target |
| `-dump-include-pii` | `false` | Include identifiers in unknown-device dumps |
| `-mdns` | `true` | Advertise GraphQL over mDNS |
| `-mdns-instance` | `helianthus` | mDNS instance label |

Required deployment inputs:

- Reachable transport endpoint at `-network` + `-address`.
- Private module access if building outside CI (`GOPRIVATE=github.com/d3vi1/*` and auth setup).

Environment variables used by deployment/smoke paths:

- `cmd/gateway`: none.
- `cmd/smoke`: set `EBUS_SMOKE=1` to enable smoke execution path.

## API readiness probes

There is currently no dedicated `/healthz` or `/readyz` endpoint. Use functional API probes.

GraphQL readiness (`200` and non-empty `data.__typename`):

```bash
curl -fsS http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  --data '{"query":"{ __typename }"}'
```

MCP readiness (`200` and JSON-RPC `result` object):

```bash
curl -fsS http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":"ready","method":"ping","params":{}}'
```

MCP tool-surface probe (`tools/list` should include built-in tools):

```bash
curl -fsS http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":"ready-tools","method":"tools/list","params":{}}'
```

## API entrypoints

### GraphQL (`POST`/`GET`)

- Endpoint: `http://<host>:<port><graphql-path>`
- Supports query/mutation schema and semantic fields (for example `devices`, `planes`, `methods`, `zones`, `dhw`, `energyTotals`)

### Subscriptions (`WebSocket` or `SSE`)

- Endpoint: `http://<host>:<port><subscription-path>`
- WebSocket subprotocols: `graphql-transport-ws`, `graphql-ws`
- SSE supported when `Accept: text/event-stream` or `?sse=1`

### Projection snapshots (`GET`)

- Endpoint: `http://<host>:<port><snapshot-path>?address=<byte>&plane=<name>`
- Response payload shape: `{ "address", "plane", "nodes", "edges" }`
- Behavior:
  - `400` on missing/invalid `address` or `plane`
  - `404` when device/projection is not present

### MCP (`POST` JSON-RPC)

- Endpoint: `http://<host>:<port><mcp-path>`
- Methods: `initialize`, `tools/list`, `tools/call`, `ping`
- Built-in tools:
  - `ebus.devices`
  - `ebus.invoke`

Example:

```bash
curl -sS http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

### mDNS advertisement

When enabled, gateway advertises GraphQL via DNS-SD service type:

- `_helianthus-graphql._tcp`

TXT records include path + transport metadata (for example `path=/graphql`).

## Smoke context and limits

Smoke coverage is intentionally opt-in and hardware-backed:

- Command: `EBUS_SMOKE=1 go run ./cmd/smoke`
- Loads repo-root `AGENT-local.md` YAML blocks (`enh`, `expected_devices`, `smoke`)
- Supports smoke profiles: `enh` and `ebusd-tcp`
- Runs read-only GraphQL/MCP checks as part of the smoke flow
- Writes JSON report to `artifacts/smoke-report.json` by default (`smoke.report_json_output` overrides)
- Supports register probe-only mode with `smoke.register_dump_probe_only=true` (requires `smoke.register_dump_target`)

Important limits:

- Not part of default `go test ./...` CI path
- Fails early when `AGENT-local.md` is missing/invalid while `EBUS_SMOKE=1`
- Requires reachable transport and local environment matching the smoke config

## Package map

- `cmd/gateway`: production gateway binary (HTTP + GraphQL + MCP + mDNS wiring)
- `cmd/smoke`: opt-in real-bus smoke runner
- `cmd/ebusdscan`: ebusd command-port helper scanner
- `graphql`: query/mutation/subscription schema + snapshot handler + semantic runtime
- `mcp`: MCP JSON-RPC server and tool bridge
- `mdns`: DNS-SD advertisement adapter
- `ui`: embedded portal UI handler/static assets
- `register_dump*.go` / `unknown_device_dump.go`: unknown-device dump generation/upload utilities
- `gateway.go` / `broadcast_listener.go`: runtime assembly + bus/broadcast wiring

## Common workflows

### 1) Start gateway with explicit transport

ENH over unix socket:

```bash
go run ./cmd/gateway -transport enh -network unix -address /var/run/ebusd/ebusd.socket
```

ENS over tcp:

```bash
go run ./cmd/gateway -transport ens -network tcp -address 127.0.0.1:9999
```

ebusd-tcp transport:

```bash
go run ./cmd/gateway -transport ebusd-tcp -network tcp -address 127.0.0.1:9999
```

### 2) Query a projection snapshot

```bash
curl -sS 'http://127.0.0.1:8080/snapshot?address=0x08&plane=system'
```

### 3) Use MCP as integration surface

- Initialize once, then call `tools/list` and `tools/call` over JSON-RPC
- Typical flow is `ebus.devices` discovery before `ebus.invoke`

## Troubleshooting

- `gateway transport dial failed`: verify `-network`/`-address`, socket permissions, and remote availability.
- `gateway transport unsupported protocol`: ensure `-transport` is one of `enh|ens|ebusd-tcp`.
- Empty `planes`/`methods`: provider matching is registry-driven; gateway command does not expose provider flags.
- `/snapshot` returns `400`: check both `address` and `plane` query params.
- `/snapshot` returns `404`: target device exists but projection for that plane is not available.
- `smoke config missing`: create/populate `AGENT-local.md` before running with `EBUS_SMOKE=1`.
- mDNS issues on restricted hosts: run with `-mdns=false` to isolate HTTP/API behavior first.

## Related repositories and references

- `helianthus-ebusgo`: https://github.com/d3vi1/helianthus-ebusgo
- `helianthus-ebusreg`: https://github.com/d3vi1/helianthus-ebusreg
- eBUS docs/architecture: https://github.com/d3vi1/helianthus-docs-ebus
- Tracking issue: https://github.com/d3vi1/helianthus-ebusgateway/issues/81
