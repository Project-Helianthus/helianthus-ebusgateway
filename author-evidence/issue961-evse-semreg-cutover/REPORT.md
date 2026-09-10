# Gateway #961 Tesla Gen3 EVSE SemReg cutover author report

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/961
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/962
- Branch: `issue/961-evse-semreg-cutover`
- Base: `b2651d73639efb7ba690bc1464d9b8b04df51e4a`
- Implementation HEAD: `ccc3bccbaef22315fcb4e929f96097f699534f82`
- Implementation tree: `dc63f43a7525128a34a8cb266cd641c83ce03758`
- Lifecycle remediation implementation HEAD: `8f39cc90c5becace4d27a2b0ad7bf05dbafac900`
- Lifecycle remediation implementation tree: `58e0abe4ee3536fb26255fc4e4b0efe0b003d39c`
- Delayed-ingestion remediation implementation HEAD: `ff83f755c6d0f98891301f0b2fdf697e389adf02`
- Delayed-ingestion remediation implementation tree: `a728abfd3acf428769ca0feff0b1ba89f935fc4f`
- Accepted documentation mapping: docs-semantic main
  `88a422896e1dc8c45a6bf629f08b8bff6115c009`, reviewed source
  `c0f105cf83229f58ef71664f9cfd30d24c1b02ac`, tree
  `8ac4aa3786298907b6bf3d71cd70d7c95a89764f`
- SemReg runtime: `f3f761bc67e10d6a65eba6c13cb4dc51002d6955`
  (`v0.0.0-20260909085241-f3f761bc67e1`)
- Native input: gateway issue #931, closed at
  https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/931;
  registry `helianthus-modbusreg v0.6.7`, peeled commit
  `57f7eb84f7d4e1173621711bf64624726c71bc75`.

The base, branch, remote head, and implementation tree were reconciled before
this report. The implementation worktree was clean and its remote branch pointed
to the same implementation HEAD. No transport connection, acquisition, request
construction, credential, deployment, device operation, live I/O, or physical
test occurred.

## Implemented semantic publication

`TeslaGen3EVSESemanticPublication` is a stateful per-asset
`helianthus.pack.evse@1.0.0` `PublicationKernel` owner. Its explicit configured
identity includes asset, source, EVSE, connector, source epoch, clock epoch, and
driver generation. Each commit binds immutable native-record evidence, source,
binding, qualified identity link, source epoch/generation, sequence and expected
semantic revision, two read services, and two read capabilities. It declares no
EVSE operation or set-current capability.

The accepted `wc3_24_44_3` mapping is field-local:

- valid persistent t7-to-t8 terminal evidence maps exactly to
  `evse.limit.configured_current`;
- provisional t25-to-t26 plus correlated t27-to-t28 maps to
  `evse.limit.allocated_current` only with the accepted operation version,
  all four nonempty native payloads, timeout `1..86399`, no inhibit, and an
  evaluation before expiry;
- missing, malformed/correlation-mismatched, zero-timeout, inhibited,
  out-of-range, or expired provisional evidence withholds only allocated current
  and records an explicit projection loss. It cannot withdraw a valid configured
  current.

Invalid persistent evidence and invalid lifecycle metadata fail before the
kernel fork can be committed. A replay or sequence collision is rejected while
the existing detached public snapshot remains unchanged.

## P1 lifecycle remediation

The two accepted P1 review findings are corrected by
`8f39cc90c5becace4d27a2b0ad7bf05dbafac900`. Stable Tesla candidate IDs now take
their next revision from the candidate in the current committed SemReg snapshot;
the first candidate is revision `1` and a subsequent accepted publication is
revision `2`. The native evidence sequence remains the caller-supplied collision
fence, while kernel fork/apply remains atomic.

Every MCP and GraphQL read now evaluates the retained immutable SemReg snapshot
at an injected, testable clock. The read derives a later monotonic point from the
sealed publication point and calls `EvaluateSnapshot`; it neither calls a native
provider nor emits a time-only lifecycle batch or fabricated replacement snapshot.
Configured current therefore remains independently exact. A valid provisional
allocation has `FreshFor=timeout` and `RetainFor=timeout+1ns`: just before expiry
it is fresh and exact; at expiry its projection is explicitly withheld with
`withheld_provisional_expired`; after expiry SemReg evaluates it as expired and
the same projection remains withheld. This is an output disposition over the
unchanged snapshot, consistent with the accepted SemReg lifecycle/projection
contract.

## P2 delayed-ingestion remediation

`ff83f755c6d0f98891301f0b2fdf697e389adf02` keeps the native receipt
coordinate (`ObservedAt`, `MonotonicNS`) distinct from the later semantic
evaluation coordinate. It derives `EvaluateMonotonic` by adding the validated
observation-to-evaluation interval to `ReceiptMonotonic`, records both pairs in
every fact candidate, and uses the evaluation coordinate as the committed and
subsequent read baseline. Thus a 30-second ingestion delay contributes to the
SemReg freshness age: a 60-second allocation is fresh at 59 seconds and stale
at its wall-clock expiry, exactly when its public projection becomes
`withheld_provisional_expired`.

This remains an evaluation of the immutable snapshot. It makes no provider
call, native observation, or time-only lifecycle batch; MCP and GraphQL retain
their parity for the delayed-ingestion boundary.

## Public contracts and boundary

- MCP: `semantic.v1.evse.current.get` returns the SemReg
  `snapshot`, `evaluation`, `selections`, and `projection` envelope. It has no
  arguments and derives its timestamp from the SemReg evaluation.
- GraphQL: fixed `SemanticEVSECurrent` with
  `PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1` returns the same four detached members via
  the existing mTLS-authenticated GraphQL handler. It rejects a different query
  shape, contract, or unauthorised asset.
- Native evidence: `modbus.v1.tesla.gen3.evse.current_limit.get` remains the
  only native Tesla record surface. No legacy semantic adapter, alias, fallback,
  comparator, shadow, or dual publication exists.
- Operations: unavailable. ACK/readback evidence proves only the bounded native
  record correlation and never grants a sender, write, charging, or control path.

The deliberately honest limitation is that the production gateway constructor
does not compose an EVSE injected-record owner. The new boundary classifier and
gateway test fail if this seam reaches production config, lifecycle, MCP provider,
or GraphQL runtime paths. Consequently the public contract is implementable and
tested through explicit injected records, while production exposure remains
unavailable until a separately scoped, qualified retained-record injection owner
is accepted. This report makes no production or live-device reachability claim.

## RED/GREEN and validation

RED-first tests define these rejection vectors before their corresponding
implementation path is accepted: malformed persistent evidence; missing
provisional evidence; zero timeout; inhibit state; expiry; replay/collision;
stable sequential candidate revisions; just-before/equal/after-expiry lifecycle
boundaries; concurrent read stability; MCP/GraphQL parity; and production
composition. The focused race suite passed:

```text
GOWORK=off go test -race ./mcp ./m2mgraphql ./cmd/gateway \
  -run 'TeslaGen3EVSESemantic|SemanticEVSE|TeslaEVSESemantic' -count=1
PASS
```

The latest focused P2 race log SHA-256 is
`85aede43021f1624258ca1a5130c7eec6ec880debb188c452bd036038a503310`.

The final full local gate passed:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

It covered Portal Node `93/93`, `go test -race -count=1 ./...`, source-selection
schema coverage, Python script suites `168 + 6 + 24 + 9 + 6 + 2 = 215`,
`golangci-lint` with `0 issues`, the Modbus RTU production transport gate, and
the existing Growatt Storage SemReg mapping gate. The passive-smoke classifier
reported `not triggered`. The final log SHA-256 is
`c8d26c02a40f88e470aac38a3836de9b42274094d7879e12274d7bae517a7ca5`.

## Gate classification

- Documentation: satisfied by the accepted docs-semantic EVSE mapping and the
  gateway runtime-provider contract update.
- Semantic/lifecycle: applicable and passed through the focused race tests and
  full repository race suite.
- Transport: source-selected existing Modbus RTU production conformance passed;
  no Tesla transport implementation changed.
- Smoke: not applicable and not triggered because the work adds no runtime
  composition, acquisition, or live route.
- Hosted CI: [run `34448798371`](https://github.com/Project-Helianthus/helianthus-ebusgateway/actions/runs/34448798371)
  completed `SUCCESS` against the exact implementation head
  `ccc3bccbaef22315fcb4e929f96097f699534f82`: terminology, lint, build, and
  test all succeeded.

The issue and PR bodies were reconciled against this state. No independent review
or merge was requested or performed.
