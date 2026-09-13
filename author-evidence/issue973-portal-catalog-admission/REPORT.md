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
- The recorded `GOWORK=off ./scripts/ci_local.sh` run completed race, lint,
  Python and the Modbus RTU transport gate but then failed closed because the
  Storage mapping gate still named the superseded SemReg runtime. Durable log:
  `author-evidence/issue973-portal-catalog-admission/ci_local-aa26889-followup.log`,
  SHA-256 `ff6ce20df1cfef40c7b8117bd5151885df6ef9a47b9b0dc527c13b5f4c73ed56`.

## Third exact-HEAD P2 correction

`ReplaceGeneration` now first identifies every successor descriptor whose
identity has a different historical canonical digest, then atomically removes
all accepted descriptors for that driver, retains unchanged staged descriptors,
and quarantines every changed identity before returning one lexically ordered
conflict error. The new regression publishes one unchanged and two changed
descriptors, verifies that neither changed descriptor remains accepted, and
verifies the forward and reversed successor inputs produce the same snapshot.

The public admission transport contract now accurately distinguishes the action
stability revision from the caller-visible catalog digest: the former excludes
only top-level `evaluation_instant`, source `evaluated_at` and
`evaluate_monotonic` coordinates, and their derived `evaluation_digest`.
Nonvolatile evaluation facts, freshness/expiry, source revisions and all other
action admission inputs remain bound and are revalidated before invocation.

- `GOWORK=off go test ./portal/contributionv1 -run 'TestRegistry(RejectsChangedDescriptorAcrossGeneration|QuarantinesEveryChangedIdentityInSuccessorGeneration)' -count=1`: PASS.
- `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off go test -race ./portal/contributionv1 ./portal/catalogv1 ./portalgraphql -count=1`: PASS.
- The matching `cmd/gateway` normal test passed with that Xcode SDK override;
  its focused race run was interrupted after the three affected Portal packages
  passed because the Delivery Lead directed an immediate return rather than an
  indefinite wait through the long-running host gateway test.
- `node --test portal/web/test/*.test.mjs`: 101 passed, 0 failed.
- `GOWORK=off GOOS=linux GOARCH=386 go build -o /dev/null ./cmd/gateway`: PASS.
- The required unmodified-host `GOWORK=off ./scripts/ci_local.sh` attempt
  reached `go build` and failed before repository code tests because the
  macOS CommandLineTools 27.0 SDK `.tbd` files contain the unsupported
  `arm64e.x1-*` architecture label for the installed linker. This is an
  OS/toolchain defect, not a source failure; the durable failure log is
  `author-evidence/issue973-portal-catalog-admission/ci_local-218ce63-p2-followup.log`,
  SHA-256 `644a3a045352ff2fda88e6d11da9130692a2f005ca64a5290a24233685445b75`.
- One complete repeat with the existing Xcode SDK path,
  `SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off ./scripts/ci_local.sh`, completed formatting, Node,
  vet/build, Linux builds, repository-wide race, Python, lint, and transport
  gates, then failed the declared Growatt Storage SemReg mapping gate with
  `pinned SemReg storage runtime is not selected`. No retry or source correction
  followed. Durable log:
  `author-evidence/issue973-portal-catalog-admission/ci_local-218ce63-p2-sdkroot.log`,
  SHA-256 `f003408accfc21e2f548f48ef3fdf8490558bb78ec98eefba373330664e3aba1`.

The branch pins accepted SemReg main
`089ed6ae9004cfba8aff27f1e54d579aeccc0b4c`, but the existing Storage and EVSE
mapping gates still required superseded `f3f761bc67e1`. Both gate pins now
match the selected accepted runtime; their public docs pins and executable
mapping cases remain unchanged. A first complete repeat proved the Storage gate
green and exposed the equivalent EVSE drift (log SHA-256
`6868382e29c486c5b1ab6ce6611b1b39dfd8e77ba95965d3772bf8a78fc70d50`).
After both corrections, the final
`SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off ./scripts/ci_local.sh`
run passed Node 101/101, build/vet, Linux builds, repository-wide race, Python
168+6+26+11+6+2, lint, Modbus RTU transport, Storage 2 outputs/13 rejects,
EVSE 6 outputs/9 rejects, and the passive-smoke gate. Final log
`author-evidence/issue973-portal-catalog-admission/ci_local-218ce63-p2-final.log`,
SHA-256 `f992988b51380e44de28b1d2a47bc400d545d28d50386a5cf4bb9bf100cdbd51`.

## Provider-owned contribution lifecycle correction

The Portal handler now takes a detached snapshot from a long-lived Gateway
contribution registry for each request. PV, Storage, and EVSE publish a complete
descriptor generation only after their existing provider startup succeeds. The
PV adapter lease retains its explicit semantic generation `1`; Storage uses its
RTU session generation; EVSE uses its retained-owner configuration generation.
Each provider close, and Tesla's existing fence/successor seam, withdraws the
exact generation before its cached reader can remain visible. Source capture is
cache-only and binds the registry revision before and after capture, returning
`ErrCatalogChanged` rather than a mixed catalog when a contribution lifecycle
changes mid-capture.

Focused normal and race regressions cover a new fixture lease beside unrelated
contributions, exact withdrawal, complete replacement sets, and the blocked
capture barrier. The direct exported `catalogv1.Composer.Publish` caller-slice
retention observation remains a P3 backlog item: it is not reachable from this
HTTP path because only deep-detached `Registry.Snapshot` manifests reach the
request-local composer.

`SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off ./scripts/ci_local.sh`:
PASS. It completed Node 101/101, build/vet, Linux builds, repository-wide race,
Python 168+6+26+11+6+2, lint, Modbus RTU transport, Storage and EVSE SemReg
mapping, and passive smoke. Durable log:
`author-evidence/issue973-portal-catalog-admission/ci_local-4d1b7ab-provider-leases.log`,
SHA-256 `12074280766d5ace1fafe02aed489d6bcdc3b1a08ab36e46e8b9f29748d54a6f`.
