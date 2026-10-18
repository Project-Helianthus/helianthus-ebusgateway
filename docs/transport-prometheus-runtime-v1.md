# Gateway transport runtime metrics v1

`helianthus-ebusgateway` appends one bounded runtime lifecycle family to the
existing `/metrics` exposition:

```text
helianthus_transport_runtime_status{protocol,state,reason,outcome} 1
```

The Gateway records this two-protocol snapshot at existing composition and
lifecycle transitions. A scrape only copies the stored in-memory values. It
does not dial, read, discover, request SHIP/SPINE data, retry, reconnect,
publish, mutate a registry, or call a Modbus or eeBUS native runtime snapshot.
The metric is therefore a Gateway runtime-lifecycle view, not a claim about a
peer, device, pairing, or semantic-value freshness.

The fixed protocols are `modbus_tcp` and `eebus`. `state` is one of
`disabled`, `starting`, `ready`, `degraded`, `retired`, or `unknown`; `outcome`
is one of `unavailable`, `pending`, `available`, or `unknown`; and `reason` is
one of `not_configured`, `none`, `startup_failed`, `shutdown`, or `unknown`.
Invalid input becomes the finite `unknown` tuple. The renderer emits exactly
one sample per listed protocol in deterministic order. It never exposes an
endpoint, interface, SKI, serial, address, credential path, peer identity,
source locator, native error text, or another installation-specific value.

`disabled` means the corresponding runtime was not configured. `degraded`
means configuration was present but Gateway could not establish the composed
runtime at startup. `ready` means the Gateway lifecycle successfully composed
the public runtime; it does not imply a live peer or qualified device. eeBUS
may remain `ready` when its optional typed operator-admin capability is
unavailable, because that capability has its own readiness surface and does
not withdraw the read-only runtime. `retired` is published before the Gateway
closes its metrics-serving control plane. Gateway atomically fences the eeBUS
metric observer at that point: existing native recovery and shutdown ownership
continues through its teardown defer, but it cannot replace the terminal metric.
The HTTP control plane retains the Gateway context values while deferring shared
cancellation until retirement is published. Teardown then performs a bounded
graceful HTTP drain and uses forced close only as a reported fallback.
Where the lifecycle cannot truthfully map a state, the metric emits `unknown`
instead of inferring health.

For example, alert on a configured runtime that is unavailable:

```promql
helianthus_transport_runtime_status{protocol="modbus_tcp",outcome="unavailable",reason="startup_failed"} == 1
```

An eeBUS deployment may alert on a prolonged reconstruction window:

```promql
helianthus_transport_runtime_status{protocol="eebus",state="starting",outcome="pending"} == 1
```

Current coverage is limited to already-composed Modbus TCP and eeBUS runtimes.
The eBUS baud availability family is described below. CAN is
`not_composed` until its own INT-07 runtime exists, so this family emits no CAN
series. Thermal and Infrastructure semantic metrics, Matter/eeBUS output
bindings, the Gateway rename, native mapping, SemReg packs, pollers, daemons,
and transport changes remain outside this contract.


## eBUS baud observation availability

For configured eBUS, `/metrics` emits this unlabelled gauge with its HELP and
TYPE metadata:

```text
# HELP ebus_baud_observation_available Whether a current qualified native baud timing observation is available.
# TYPE ebus_baud_observation_available gauge
ebus_baud_observation_available 0
```

The store captures eBUS configuration presence at construction, before filling
in transport defaults: an injected active or passive eBUS transport, an explicit
transport protocol, or a non-empty transport address establishes presence.
The production lifecycle preserves this fact before injecting stable driver
wrappers; those wrappers do not establish configuration presence themselves.
An empty configuration, Modbus-only configuration, or eeBUS-only configuration
omits the entire baud family, including HELP, TYPE, and samples. Configuration
presence is independent of readiness, traffic, transport counters, and shutdown;
a configured but stopped eBUS still emits availability `0`.

Absence means eBUS was not configured for this store. Zero means eBUS was
configured but no current qualified native timing observation is available. The
current transport snapshot contains cumulative ingress and fault counters, not
such a timing window, so the measured baud value is omitted. Configured baud,
received-symbol counts, application timing, reconnects, and resets cannot
establish a measured baud value or change availability to `1`. Rendering this
family uses the stored configuration fact without dialing, reading transport
counters, or performing device I/O. This does not change native evidence,
qualification, admission, or MCP/GraphQL contracts. The historical `v1` page name
does not promise stability before Helianthus 1.0.
