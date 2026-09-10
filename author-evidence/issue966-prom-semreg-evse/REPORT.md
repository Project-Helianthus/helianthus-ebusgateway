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

## P2 scrape freshness-floor correction

Source `8ae98a05c0252d1d612bce80bd767c6eea2cddbc` adds a Prometheus-only
monotonic high-water floor per accepted publication. A scrape at allocated
current expiry therefore withholds that connector, and a later wall/monotonic
reorder to one second before expiry evaluates at the retained floor instead of
resurrecting it. The floor does not alter the immutable snapshot, source
sequence, revision, or native lifecycle. A valid later accepted publication
replaces the floor and can expose its own fresh allocation. The regression also
proves configured current remains exact and scraping does not invoke a
publication clock or native I/O.

- Focused race passed on the source tree: `GOWORK=off go test -race ./mcp -run
  TestTeslaGen3EVSESemanticPublicationPrometheus -count=1`, plus root and
  gateway semantic renderer tests.
- `GOWORK=off ./scripts/ci_local.sh` — PASS: Portal 93/93, complete Go race,
  Python 168/6/26/11/6/2, lint 0, both mapping gates PASS, and transport and
  passive-smoke gates not triggered. Durable log
  `/tmp/helianthus-ebusgateway-966-p2-full-ci.log`, SHA-256
  `71e1867afd16a359c2b93f572a3eaa3f7320813055362abf4f68bcf0a3db5923`.

## P2 accepted-evidence aging correction

Source `a9f77f7519bc2ac2ee2aa5927fe31d7366d01389` (tree
`35af57e410bd47359a980be2930ba8fa928c9a1a`) carries the elapsed interval
between immutable `EvaluatedAt` and the accepted publication epoch into the
Prometheus-only monotonic scrape floor. An immediate scrape can therefore not
present a 60-second allocation as exact after delayed publication. The
deterministic regression covers publication at 59 seconds (exact), 60 seconds
(withheld), and 120 seconds (withheld), plus a later rollback scrape. It also
proves the configured current remains independently exact where appropriate,
and that neither scrape nor the rollback path calls a publication clock or
native I/O. The existing high-water and successor-publication tests retain the
per-publication reset and connector non-resurrection properties.

- `GOWORK=off go test -race -count=1 . ./cmd/gateway ./mcp` — PASS: root
  9.575s, gateway 96.017s, MCP 25.554s. Durable log
  `/tmp/helianthus-ebusgateway-966-delay-focused-race.log`, SHA-256
  `d91e5249fdff2f0633f32090ff379f2bec43f6c34dc36c1848eacf5b592de973`.
- `GOWORK=off ./scripts/ci_local.sh` — PASS: Portal 93/93; complete Go race
  suite; Python 168/6/26/11/6/2; golangci-lint 0 issues; transport gate not
  triggered; Growatt Storage mapping 2 outputs/13 rejected; Tesla EVSE mapping
  6 outputs/9 rejected; passive-smoke gate not triggered. Durable log
  `/tmp/helianthus-ebusgateway-966-delay-full-ci.log`, SHA-256
  `05ac9fd606af1bf98cc8d8bf523c6c0c902431979876acff0d0f9b241edddb20`.
