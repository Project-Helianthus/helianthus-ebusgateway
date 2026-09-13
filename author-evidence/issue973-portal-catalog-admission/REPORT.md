# Issue 973 Portal catalog admission v1 author evidence

## Initial candidate

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: #973
- Branch: `issue/973-portal-catalog-admission`
- Base: `0828afa6221197c01cca85abc2344d2b41899b92`
- Initial candidate: `08425eea4c87c0eeb7ab6236fef56b22a820e5c9`
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
capture barrier.

`SDKROOT=/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk GOWORK=off ./scripts/ci_local.sh`:
PASS. It completed Node 101/101, build/vet, Linux builds, repository-wide race,
Python 168+6+26+11+6+2, lint, Modbus RTU transport, Storage and EVSE SemReg
mapping, and passive smoke. Durable log:
`author-evidence/issue973-portal-catalog-admission/ci_local-4d1b7ab-provider-leases.log`,
SHA-256 `12074280766d5ace1fafe02aed489d6bcdc3b1a08ab36e46e8b9f29748d54a6f`.

## Final complete-feedback correction

The complete live feedback read found two connector P2 reports submitted after
the prior inventory snapshot. Registry generation is now a retained high-water
mark separate from active membership. Exact withdrawal clears active membership
and descriptors while rejecting delayed stale/equal generations; a duplicate
withdrawal is a no-op, and a later successor generation can activate normally.

`catalogv1.Composer.Publish` now canonicalizes and deep-detaches its input before
storage. Replaying the identical canonical digest returns without changing the
revision, while divergent bytes retain the existing quarantine behavior. The
same regression mutates the caller's nested action label after publication and
proves the stored descriptor is unchanged. This also closes the earlier P3
caller-slice backlog rather than carrying it forward.

The first full run after these changes was interrupted once the duplicate-
withdrawal review exposed the missing active-membership distinction and is not
acceptance evidence. Focused race then passed for contribution registry,
composer and Gateway Portal lifecycle tests. The final complete SDK-backed CI
passed Node 101/101, build/vet, Linux builds, repository-wide race, Python
168+6+26+11+6+2, lint,
Modbus RTU transport, Storage and EVSE SemReg mapping, and passive smoke. Final
log `author-evidence/issue973-portal-catalog-admission/ci_local-07943ef-final-all-feedback.log`,
SHA-256 `b7a990166a8e7d911ee3bb19a6329a8c83c9cd9d90d6c09509e68b4d805f65e9`.

## Final registry cross-path correction

The next connector review supplied two more P2 findings against the previous
provider-lifecycle head. Exact generation withdrawal now removes conflict-only
quarantine keys as well as accepted descriptors. The legacy `Accept` admission
path now removes a generation-accepted descriptor when divergent canonical
content quarantines the same identity, and increments the registry revision so
an in-flight catalog capture detects the visible transition. Regressions prove
conflict-only withdrawal, exclusive accepted-or-quarantined state across
`ReplaceGeneration` then `Accept`, and the revision fence.

Focused race validation passed for contribution registry, composer and Gateway
Portal lifecycle tests. The final complete SDK-backed CI passed Node 101/101,
build/vet, Linux builds, repository-wide race, Python 168+6+26+11+6+2, lint,
Modbus RTU transport, Storage and EVSE SemReg mapping, and passive smoke. Final
log `author-evidence/issue973-portal-catalog-admission/ci_local-157a8ca-all-18-feedback.log`,
SHA-256 `5a7bc7a8e026327630a1acbd31edb0ff86e67775acd3831c2519eddf07bf7cb8`.

## Final action-binding and sticky-quarantine correction

The latest complete feedback read found two further P2 reports. Catalog action
composition now requires the source resource to match the descriptor's exact
driver/manifest/version tuple and the action's exact service/capability
context. Missing context fails closed. A table-driven regression varies every
member independently and proves no mismatched source can expose an action or
lend its provenance fence to another contribution.

Generation conflict quarantine is now sticky for an identity until the exact
active generation is withdrawn. A later successor carrying either the changed
or original historical digest remains quarantined and cannot restore an
accepted descriptor. The regression exercises original A, conflicting B, then
original A again and requires zero accepted descriptors and one quarantine.

Focused normal and race validation passed for `portal/catalogv1` and
`portal/contributionv1`. The final complete SDK-backed CI passed Node 101/101,
build/vet, Linux builds, repository-wide race, Python 168+6+26+11+6+2, lint,
Modbus RTU transport, Storage 2 outputs/13 rejects, EVSE 6 outputs/9 rejects,
and passive smoke. Final log
`author-evidence/issue973-portal-catalog-admission/ci_local-2517038-all-20-feedback.log`,
SHA-256 `bbb03e99a5a49bf70bf1d16bb378d674694cf2a8c518ad2f4c1dc37a2d8c9e50`.

## Final numeric-revision and fixed-domain schema correction

The subsequent connector review supplied two P2 findings against the previous
head. Evaluation normalization now decodes JSON with `UseNumber`, so removing
volatile clock fields cannot round or alias non-clock integers above `2^53`.
The regression varies adjacent large counter values while every source fence is
stable and requires different action revisions; the existing clock-only test
continues to require a stable revision.

The v1 catalog schema now accepts exactly the producer's canonical ordered five
domain tuples and exact pack versions. It rejects a sixth domain, a duplicate,
an arbitrary pack ID, and a wrong version. The schema regression evaluates the
actual `prefixItems`, `allOf`, `const`, `minItems`, and `maxItems` constraints.

Focused normal and race validation passed for `portal/catalogv1`. The final
complete SDK-backed CI passed Node 101/101, build/vet, Linux builds,
repository-wide race, Python 168+6+26+11+6+2, lint, Modbus RTU transport,
Storage 2 outputs/13 rejects, EVSE 6 outputs/9 rejects, and passive smoke. Final
log
`author-evidence/issue973-portal-catalog-admission/ci_local-a8665ed-all-22-feedback.log`,
SHA-256 `63f747ff390474b2b80531f22433b288e89ffccb72841a5520369d2d187ef728`.

## Final cross-admission history and no-op withdrawal correction

The next connector review supplied two P2 findings against the preceding head.
`Registry.Accept` now records its trusted canonical digest in the same retained
history consulted by `ReplaceGeneration`. Divergent content submitted through
the reverse `Accept(A)` then first `ReplaceGeneration(B)` order is quarantined;
an identical descriptor may enter the first generation normally. The regression
requires exclusive accepted-or-quarantined state in both cases.

`Composer.Withdraw` now increments its revision only when the exact descriptor
or quarantine existed. Repeating a withdrawal or naming an absent identity is a
no-op and cannot invalidate an issued action claim; removing a present identity
still advances the revision. Focused normal and race regressions cover all three
transitions.

The final complete SDK-backed CI passed Node 101/101, build/vet, Linux builds,
repository-wide race, Python 168+6+26+11+6+2, lint, Modbus RTU transport,
Storage 2 outputs/13 rejects, EVSE 6 outputs/9 rejects, and passive smoke. Final
log
`author-evidence/issue973-portal-catalog-admission/ci_local-bf521f7-all-24-feedback.log`,
SHA-256 `b575ce1d37f2f342269d588b204fa9b2de147b30893a771b86b722695243b825`.
