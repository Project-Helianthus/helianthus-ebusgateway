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
eBUS keeps its existing transport/runtime exposition unchanged. CAN is
`not_composed` until its own INT-07 runtime exists, so this family emits no CAN
series. Thermal and Infrastructure semantic metrics, Matter/eeBUS output
bindings, the Gateway rename, native mapping, SemReg packs, pollers, daemons,
and transport changes remain outside this contract.
