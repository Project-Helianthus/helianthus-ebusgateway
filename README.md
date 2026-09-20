# helianthus-ebusgateway

`helianthus-ebusgateway` is the runtime composition and API edge for Helianthus.
It hosts enabled protocol-native drivers, composes their promoted semantic state,
and exposes declared GraphQL, MCP, Portal, Prometheus, subscription, and operator
surfaces. The repository keeps its historical name while its runtime scope spans
more than eBUS.

## Purpose and Scope

### What belongs in this repository

- Gateway runtime assembly (`gateway.go`, `cmd/gateway`).
- Existing typed driver lifecycle, configuration, admission, and generic
  contribution seams. The universal public lifecycle/configuration service is
  still in progress and is not exposed by the current binary.
- Native and semantic GraphQL query/mutation/subscription surfaces (`graphql/`,
  `m2mgraphql/`).
- Native and semantic MCP JSON-RPC tool surfaces (`mcp/`).
- Portal, Prometheus, optional UI mount, and mDNS advertisement (`portal/`,
  `semantic_prometheus.go`, `ui/`, `mdns/`).
- Hardware-backed smoke entrypoint and unknown-device dump plumbing (`cmd/smoke`, `smoke*.go`, `register_dump*.go`).

### Gateway command map

`cmd/gateway/main.go` keeps process entry and runtime composition. Its same-package
companions group the existing command declarations by responsibility:

- `gateway_cli.go` -- flags, CLI parsing, and command-line normalization.
- `gateway_admission.go` -- startup admission and observe-first helpers.
- `gateway_adapter_direct.go` -- adapter-direct wiring and proxy callbacks.
- `gateway_v8_admin_http.go` -- V8 diagnostic event surface.
- `gateway_http_server.go` -- HTTP, GraphQL, MCP, UI, Portal, and mDNS assembly.
- `gateway_portal_projection.go` -- Portal semantic projection helpers.
- `gateway_responder_capability.go` -- responder capability projection.
- `gateway_static_seed.go` -- static registry seed bootstrap.
- `gateway_runtime_state.go` -- runtime-state bootstrap and readiness projection.

The defensive non-send Tesla Gen3 HSC completed-outcome owner is documented in
[`docs/tesla-gen3-hsc-retained-owner.md`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/tesla-gen3-hsc-retained-owner.md).

### What does not belong in this repository

- Low-level transport framing and bus primitives (use `helianthus-ebusgo`).
- Reusable protocol-native registry definitions, profile qualification rules,
  decoders, evidence schemas, and transport primitives (use the owning native
  registry and transport repository, such as `helianthus-ebusreg`,
  `helianthus-eebusreg`, or `helianthus-modbusreg`). Gateway-owned drivers and
  adapters still compose those contracts and retain runtime-native evidence;
  they do not redefine the upstream protocol contract.
- Canonical protocol-neutral semantic types and publication contracts (use
  [`helianthus-semreg`](https://github.com/Project-Helianthus/helianthus-semreg)).
- Platform deployment bundles or auth/TLS edge policy management (handled by deployment infrastructure).

## Status and Maturity

- Active gateway service with repository CI and race-enabled tests.
- The accepted Gateway baseline pins SemReg at
  [`089ed6ae9004`](https://github.com/Project-Helianthus/helianthus-semreg/commit/089ed6ae9004cfba8aff27f1e54d579aeccc0b4c).
- Current accepted SemReg composition covers PV, Storage, and EVSE domain paths;
  Thermal/HVAC ([issue #952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952)),
  Infrastructure, complete EEBUS output, Matter output, packaging, and physical
  validation remain separate or in-progress 0.7 work.
- Smoke mode is intentionally opt-in and environment-backed (`EBUS_SMOKE=1` + local config file). Repository and offline tests do not establish physical qualification.

### Accepted public evidence

| Surface | Accepted revision and boundary |
|---|---|
| SemReg PV composition | [PR #956 / `2ad927b`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/956) makes one SemReg PV publication available to MCP, M2M GraphQL, and Portal. |
| SemReg Storage and EVSE | [PR #959 / `b2651d7`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/959) and [PR #962 / `1fbf5e1`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/962) add the accepted read-only domain paths. Producer availability and physical qualification remain separate. |
| Prometheus | [PR #964 / `c139d0e`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/964) exports bounded PV and Storage projections, [PR #967 / `2daae4c`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/967) adds EVSE, and [PR #978 / `143bf19`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/978) adds detached Modbus and EEBUS runtime status. |
| Generic Portal contributions | [PR #984 / `5eb5346`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/984) renders the admitted read-only contribution catalog with explicit absence, provenance, freshness, quality, lifecycle, and projection-loss states. Complete INT-10 remains open. |
| Home Assistant consumers | The separate integration consumes the public PV, Storage, and EVSE SemReg contracts in [PR #257](https://github.com/Project-Helianthus/helianthus-ha-integration/pull/257), [PR #261](https://github.com/Project-Helianthus/helianthus-ha-integration/pull/261), and [PR #263](https://github.com/Project-Helianthus/helianthus-ha-integration/pull/263). Those merges do not prove the packaged add-on contains the same revisions. |
| EEBUS and Matter | Both are public 0.7 software scope and do not depend on private hardware. EEBUS-native runtime/read surfaces are present, while the protocol-neutral EEBUS output remains in progress. The accepted baseline has [no composed Matter output binding](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/5eb53465e88d254455251203e2dc30f813541bba/docs/architecture/runtime-driver-provider-contract-v1.md#current-operation-inventory-at-the-pinned-baseline); it makes no Matter conformance claim. |

These links establish merged software and offline validation only. They do not
establish packaging parity, installation success, certification, device support,
or physical validation.

## Stable Instance Identity

- The gateway can expose an installation-scoped stable GUID with `-instance-guid <uuid>`.
- When configured, the same GUID is published through GraphQL at `gatewayIdentity.instanceGuid`.
- Zeroconf advertisement keeps `_helianthus-graphql._tcp` and adds TXT `instance_guid=<uuid>`.
- Home Assistant should treat this GUID as canonical identity and treat `host`, `port`, `path`, and `transport`
  as rediscoverable transport coordinates.

## Helianthus Dependency Chain

```text
protocol-native transports and registries
                 -> helianthus-semreg -> helianthus-ebusgateway -> consumers
                    (canonical state)    (composition/APIs)
```

Native evidence remains available beside the promoted path; SemReg does not
replace protocol owners, and the Gateway does not redefine their evidence.

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

curl -fsS http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  --data '{"query":"{ gatewayIdentity { instanceGuid } }"}'

curl -fsS http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":"ready","method":"ping","params":{}}'
```

Portal API probes and operational notes live in docs:
https://github.com/d3vi1/helianthus-docs-ebus/blob/main/api/portal.md

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
| `-portal-path` | `/portal` | set empty to disable dynamic portal surface |
| `-instance-guid` | _empty_ | lowercase UUIDv4 published via GraphQL and Zeroconf |
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
- Portal API and operations: https://github.com/d3vi1/helianthus-docs-ebus/blob/main/api/portal.md
- Smoke-test documentation: https://github.com/d3vi1/helianthus-docs-ebus/blob/main/development/smoke-test.md
- Issue tracker: https://github.com/d3vi1/helianthus-ebusgateway/issues

### Issue workflow conventions

- Use one issue-focused branch (example: `issue-93-readme-refresh`).
- Keep PR scope aligned to issue acceptance criteria.
- Include closing keyword in PR body (example: `Fixes #93`).
- Request agent review comment after opening the PR (`@codex review`).
