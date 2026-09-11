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

## Exact-HEAD follow-up correction

The independent exact-HEAD review of `d0ad038` identified four additional
blocking cases. Catalog revision hashing now excludes only response-time
`evaluation_instant` and response digest fields, so a fresh invocation capture
does not reject an otherwise unchanged action solely because wall time advanced.
Source/descriptors/caller/fence records remain bound, and revalidation still
owns freshness and expiry checks before native I/O.

`ReplaceGeneration` now deep-detaches the validated canonical manifest before
storage, compares the complete same-generation descriptor key set, and rejects
both added and omitted replay members. The route guard now checks the normalized
control-plane plan, including no-leading-slash UI and dump-upload paths. New
regressions prove action success across changed response time, immutable nested
manifest slices, complete replay key-set equality, and normalized route
collisions. The older complete-tuple ordering and group-resource-context
regressions remain covered.

- Follow-up focused race tests: PASS.
- Portal Node suite: 101 passed, 0 failed.
- Linux 386 package compilation: PASS; generated binaries were moved outside
  the worktree.
- Follow-up `GOWORK=off ./scripts/ci_local.sh`: PASS, including race, lint,
  Modbus RTU transport, and passive mapping gates. Durable log:
  `author-evidence/issue973-portal-catalog-admission/ci_local-d0ad038-followup.log`,
  SHA-256 `ab158b10d536dd82571f2a4023a8bb30bf1b948b3be95c7aecc54ecf1c9da59a`.

## Second exact-HEAD follow-up

The next connector review found that the action-stability view still included
the detached source evaluation clock and its derived digest. It now normalizes
only those volatile clock coordinates and the derived digest for the catalog
revision, while retaining facts, dispositions, loss, quality, provenance,
source, binding, epoch, generation and eligibility records. A source-backed
advancing-clock invocation regression proves the native invoker remains
reachable when only those clock values advance.

The contribution registry retains historical canonical digests across a driver
generation boundary: an unchanged descriptor is accepted by a successor
generation; changed bytes for the same identity are quarantined instead of
silently replacing the prior value. The catalog JSON Schema now has closed item
schemas for every populated array and source/ref-bearing record, with a
populated and malformed-document regression. The five-domain fixture and tests
now use the accepted `helianthus.pack.*` IDs.

- Focused normal/race/schema tests: PASS.
- Portal Node suite: 101 passed, 0 failed.
- Linux 386 package compilation: PASS; generated binaries were moved outside
  the worktree.
- `GOWORK=off ./scripts/ci_local.sh`: PASS, including race, lint, Modbus RTU
  transport, and passive mapping. Durable log:
  `author-evidence/issue973-portal-catalog-admission/ci_local-aa26889-followup.log`,
  SHA-256 `ff6ce20df1cfef40c7b8117bd5151885df6ef9a47b9b0dc527c13b5f4c73ed56`.
