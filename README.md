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

## Configuration (gateway command)

`cmd/gateway` is flag-driven (no environment-variable parsing in this command).

Core transport/runtime flags:

- `-transport`: `enh` | `ens` | `ebusd-tcp`
- `-network`: dial network (`unix` or `tcp`)
- `-address`: socket path or `host:port`
- `-read-timeout`, `-write-timeout`, `-dial-timeout`
- `-queue-capacity`: bus queue size (`0` = protocol default)
- `-broadcast`: start separate broadcast listener connection

HTTP/API flags:

- `-http-addr` (default `:8080`; empty disables HTTP server)
- `-graphql-path` (default `/graphql`)
- `-snapshot-path` (default `/snapshot`)
- `-subscription-path` (default `/graphql/subscriptions`)
- `-mcp-path` (default `/mcp`)
- `-ui-path` (default `/ui`)
- `-dump-upload-path` (disabled unless set)
- `-dump-output-dir` (default `./dumps`)

mDNS flags:

- `-mdns` (default `true`)
- `-mdns-instance` (default `helianthus`)

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

## Smoke test context and limits

Smoke coverage is intentionally opt-in and hardware-backed:

- Command: `EBUS_SMOKE=1 go run ./cmd/smoke`
- Reads local `AGENT-local.md` YAML blocks for transport + expected devices + behavior
- Designed for real-bus validation (scan, read-only invoke flows, optional register dump/probe)

Important limits:

- Not part of default `go test ./...` CI path
- Fails early when `AGENT-local.md` is missing/invalid while `EBUS_SMOKE=1`
- Requires reachable bus transport and matching local environment

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
