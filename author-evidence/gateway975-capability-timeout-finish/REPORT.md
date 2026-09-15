# Gateway #975 Portal capability timeout correction

Repository: `helianthus-ebusgateway`

Issue/branch: #552 / `issue/552-portal-ux`
Base: `7a1dd16c69244a6944da5f521c33b32a59342913`

## Scope

Fix valid finding `r4012186740`: a stalled `vaillantCapabilities` GraphQL
request could leave the selected B503 target in `PENDING` indefinitely, while
navigation suppressed the retry that would recover it.

`PortalShell` now gives each capability qualification a five-second bounded
request with cancellation. A target change, same-target Portal re-entry, and
component teardown invalidate and abort an older request. Request version and
qualification context remain the authority for rendering, so an aborted or late
probe cannot overwrite the current target. A timeout renders conservative
`UNKNOWN`, from which re-entry can qualify again.

## Deterministic regression control

`VaillantB503Pane_stalledQualificationTimesOutAndReentrySupersedesStaleProbe`
uses the Portal fake timer and deferred fetches to prove all of the following:

1. a stalled probe installs one five-second timeout and is aborted;
2. timeout releases `PENDING` to visible `UNKNOWN`;
3. same-target re-entry starts exactly one replacement probe and aborts the old
   probe; and
4. a late old reply cannot overwrite the replacement result.

The control would fail against the base implementation because it installs no
capability timeout and keeps the original fetch pending.

## Validation

| Command | Result |
| --- | --- |
| `node --test portal/web/test/vaillant-b503.test.mjs` | PASS, 63 tests |
| `node --test portal/web/test/*.test.mjs` | PASS, 158 tests |
| `./scripts/build_portal_assets.sh` | PASS; regenerated Portal assets |
| `./scripts/check_portal_assets.sh` | PASS; generated assets match source |
| `git diff --check` and `git diff --cached --check` | PASS |

No live action, adaptermux/internal change, review action, feedback resolution,
or merge was performed.
