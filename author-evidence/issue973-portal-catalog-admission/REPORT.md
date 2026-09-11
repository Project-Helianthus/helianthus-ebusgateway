# Issue 973 Portal catalog admission v1 author evidence

## Candidate

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: #973
- Branch: `issue/973-portal-catalog-admission`
- Base: `0828afa6221197c01cca85abc2344d2b41899b92`
- Candidate: `08425eea4c87c0eeb7ab6236fef56b22a820e5c9`
- Tree: `53ee08ae8286c7f3438aed89eb9f2d576ab1560b`
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/974

The tracked change is 15 files, 650 insertions and 5 deletions. It adds the
closed dedicated `POST /graphql/portal/v1` route, immutable catalog package,
fixed operation-name handler, five-pack absent-source catalog, descriptor
digest quarantine, caller-scoped action filtering and pre-invocation revalidation
seam. Existing `/graphql` is not altered. Portal bootstrap advertises the new
generic endpoint once.

## RED and focused green evidence

The initial focused build exposed the handler's erroneous `MaxBytesReader`
argument before tests could run; it was corrected to use `r.Body`. Focused
green evidence after the correction:

- `GOWORK=off go test ./portal/catalogv1 ./portalgraphql ./portal ./cmd/gateway -run 'Test(Catalog|Conflict|Portal|HTTPControlPlaneRouteManifest)' -count=1`: pass, 4 packages.
- `GOWORK=off go test -race ./portal/catalogv1 ./portalgraphql`: pass, 2 packages.
- `node --test portal/web/test/*.test.mjs`: 101 passed, 0 failed.
- `GOOS=linux GOARCH=386 GOWORK=off go test -c` for both new packages: pass.
- JSON Schema and catalog fixture parsed successfully with `python3 -m json.tool`.

## Complete gate

`GOWORK=off ./scripts/ci_local.sh` passed from this standalone worktree before
the commit. The tracked source tree was unchanged between that gate and commit.

- Log: `author-evidence/issue973-portal-catalog-admission/ci_local.log`
- SHA-256: `46d30d4b6f53f99039af04e165294cb7475b09c06f3ffe713b6235d4fe1901fa`

## Residual limits

No INT-10 renderer, legacy compatibility/fallback, native acquisition,
production Portal identity adapter, live control, device, deployment, or
physical verification is included. The default runtime catalog intentionally
reports source-absent domains and no actions until an accepted detached-source,
caller authorization, SemReg/binding and DriverManager adapter is configured.

## Completion correction

The original candidate correctly added the dedicated closed route but left its
production composition deliberately empty. The first recovery pass also exposed
the four independent review findings: populated wire records lacked snake-case
tags; tuple ordering could depend on map iteration; actions used a group ID
instead of its resource context; and a configured route could collide with the
fixed catalog route. These are corrected in the final worktree.

The completed path has a Gateway-owned enumerable generation-fenced descriptor
registry, a thin `packs.NewMetadataRegistry` semantic resolver, and validated
PV, Storage, and EVSE contribution descriptors. Gateway lifecycle constructs
the detached source explicitly from the existing PV
`SemanticPVCurrentSingleAt`, Storage `CurrentAt`, and EVSE
`SemanticEVSECurrentAt` owners and passes it into `startHTTPServer`; it does not
recover those owners from the MCP provider. The catalog retains each complete
SemReg snapshot, evaluation, selections, and projection record as detached
wire data, and does not create a null field-value surrogate. Storage uses the
accepted `storage.state.soc` / `unit.percent` identifiers.

Focused evidence after this correction:

- `GOWORK=off go test -race ./portal/contributionv1 ./portal/catalogv1 ./portalgraphql ./cmd/gateway -run 'Test(Registry|PortalCatalog|Catalog|Conflict|InvokeDoesNot|HTTPControlPlaneRouteManifest)' -count=1`: PASS.
- `node --test portal/web/test/*.test.mjs`: 101 passed, 0 failed.
- `GOOS=linux GOARCH=386 GOWORK=off go test -c` for contribution, catalog, and transport packages: PASS. Generated binaries were moved outside the worktree before the final gate.
- Final `GOWORK=off ./scripts/ci_local.sh`: PASS. It includes formatting, Node, vet, Linux 386, repository-wide race tests, Python checks, lint, the declared Modbus RTU transport gate, and passive mapping checks. Durable log: `author-evidence/issue973-portal-catalog-admission/ci_local-final-rerun.log`, SHA-256 `12a8d5298f0c23ec071200b2cbce122b49b7feef13a90596f65c4ba9aa0e3608`.

The prior full-gate attempts are retained as evidence: the first stopped at an
unformatted preserved test; the second reached lint and reported one
`staticcheck` conversion cleanup in the new registry snapshot. The final rerun
passes both. No live, device, deployment, credential, or native-control action
was performed.
