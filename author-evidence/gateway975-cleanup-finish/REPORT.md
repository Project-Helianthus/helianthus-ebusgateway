# PR 975 cleanup-finish author evidence

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: #552
- Branch: `issue/552-portal-ux`
- Starting pushed HEAD: `24e20b50b821c47bc6992b9b12f2a509c7236068`
- Feedback resolved: `r4011811492`, `r4011847795`, `r4011847800`, and
  `r4011847804`

## Completed correction

The session Manager now distinguishes a normal owner Disable from a defensive
cleanup Disable. Reads and normal Disables are admitted only while the session
is `Active` and still owns the gate. The sole ownerless admission is a
Manager-created cleanup pending operation in `Disabled`, after an emitted
lifecycle operation left settlement unproven. That pending operation is
cancelable; an epoch advance invalidates it before the dispatcher's
pre-emission admission, preventing an old cleanup from emitting through a new
transport epoch.

Post-emission Enable failure cleanup runs one conservative, target-bound
attempt with a fresh five-second background context. It does not inherit a
cancelled or expired request context, and it retains exact native outcome
evidence without treating an ACK as settlement.

Portal nav-away cleanup remains detached from the tab swap. A confirmed
immediate or deferred cleanup for the current selected target first publishes
`UNKNOWN`, then performs one target-bound capability refresh without a
visible-pane session request. The retained explicit picker target remains
enabled after discovery becomes empty because the pane and picker both disable
only when discovery is empty and the canonical target is null. Generated Portal
assets were rebuilt with the source change.

## Regression evidence

- `GOWORK=off CGO_ENABLED=0 go test -race -count=1 ./internal/vaillant/b503session`:
  PASS. New deterministic tests cover epoch cancellation before defensive
  cleanup admission/no write and fresh-context cleanup after caller
  cancellation with exactly one attempt.
- `GOWORK=off CGO_ENABLED=0 go test -race -count=1 ./cmd/gateway -run 'B503'`:
  PASS.
- `GOWORK=off CGO_ENABLED=0 go test -race -count=1 ./graphql ./mcp -run 'B503'`:
  PASS.
- `node --test portal/web/test/vaillant-b503.test.mjs`: PASS, 62 tests.
  The added confirmed-tab-cleanup regression proves the tab changes before the
  detached cleanup settles, sends one disable and one capability request, sends
  no session request, and fences cached `AVAILABLE` state as `UNKNOWN`.
- `node --test portal/web/test/*.test.mjs`: PASS, 157 tests.
- `./scripts/check_portal_assets.sh`: PASS after staging rebuilt generated
  assets; `app.js` SHA-256 is
  `a383a6d8def34637ed7fb7dc20d2e2979dfe4df162ede67d0c6cd010f48a6ead`.
- Staged and unstaged `git diff --check`: PASS.

No live device, deployment, credential, installation, transport conformance,
or merge action was performed. The declared T01..T88 release deferral remains
outside this focused correction.
