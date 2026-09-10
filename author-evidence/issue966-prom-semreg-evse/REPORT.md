# Gateway #966 EVSE SemReg Prometheus author report

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/966
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/967
- Branch: `issue/966-prom-semreg-evse`
- Base: `c139d0e6ae59b4f925ac02a7cadf09db53278e13`
- Validated implementation HEAD: `5f9ff3bfb47652473ff7af098858cdb6ed9d6e77`
- Validated implementation tree: `5524a3ad10cb645fc75001e4cefd80bf19c5f697`

## Implemented scope

The existing `/metrics` renderer now accepts the bounded `evse` SemReg domain.
It renders only `helianthus.pack.evse@1.0.0` configured and allocated current
facts with their exact v1 fact, dimension role, quantity kind, and ampere unit.
Configured current remains independent when allocated current is withheld.
Connector identity is converted into a deterministic eight-slot semantic label
vocabulary and no raw identity, source, binding, serial, endpoint, evidence,
digest, policy, or free text is emitted.

`TeslaGen3EVSESemanticPublication.SemanticEVSECurrentAt` reads one immutable
snapshot under a read lock and reevaluates it at the caller-provided scrape
instant. It does not call a native endpoint, publication clock, read clock, or
`Publish`; snapshot-floor handling and wall rollback remain fail-closed.
Concurrent publication and scrape coverage proves every returned tuple binds
one snapshot, evaluation, and projection revision.

`PrometheusEVSEEnabled` is disabled by default. When explicitly enabled but no
future detached EVSE runtime is composed, `/metrics` reports bounded EVSE
availability zero. This issue adds no Tesla acquisition, native operation,
Portal, GraphQL, MCP call, fallback, dual publication, deployment, credential,
or production #965 claim.

The public binding contract is documented in
`docs/semantic-prometheus-evse-v1.md`; its accepted mapping input is
https://github.com/Project-Helianthus/helianthus-docs-semantic/tree/88a422896e1dc8c45a6bf629f08b8bff6115c009.

## Validation

- `GOWORK=off go test -race -count=1 . ./cmd/gateway ./mcp` — PASS: root
  9.592s, gateway 96.818s, MCP 18.899s. Durable log
  `/tmp/helianthus-ebusgateway-966-focused-race.log`, SHA-256
  `38e08ac41ee47fead76fac5ce16f497b1c5eab8c925253f3e599bbaefa2eb558`.
- `GOWORK=off ./scripts/ci_local.sh` — PASS: Portal 93/93; complete Go race
  suite; Python 168/6/26/11/6/2; golangci-lint 0 issues; transport gate not
  triggered; Growatt Storage mapping 2 outputs/13 rejected; Tesla EVSE mapping
  6 outputs/9 rejected; passive-smoke gate not triggered. Durable log
  `/tmp/helianthus-ebusgateway-966-full-ci.log`, SHA-256
  `5f2431289ee36b75766f32adaaac815d8f7625a9790217a616b6d0b1fa9928e7`.

The first complete CI attempt failed before the corrected transport classifier;
the second reached an unrelated passive-canary fixture timeout which passed on
an immediate isolated rerun. The final complete run above is the acceptance
result.

Documentation gate: required and satisfied by the gateway-local EVSE metrics
contract. Transport gate: no transport behavior changed; exact configuration
and lifecycle classifier tests fail closed for extra or near-match changes.
Smoke: offline deterministic tests only; no live device, credential, native
acquisition, deployment, or hardware action occurred.
