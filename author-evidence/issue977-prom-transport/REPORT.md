# Issue #977 author evidence

Base: `7b82440fd39ed4b7d1cb63a688fa33b312f6ae5d` / tree
`8ee366b7c9fd0b9287ee469c2b8c909a691e918a`.

This candidate exposes `helianthus_transport_runtime_status` for exactly the
already-composed `modbus_tcp` and `eebus` Gateway runtimes. The renderer copies
a finite two-entry Gateway-owned snapshot. It has no native runtime callback and
cannot dial, read, discover, request SHIP/SPINE data, retry, reconnect, publish,
mutate, or take a native runtime snapshot during a scrape. Lifecycle integration
records finite values at composition, eeBUS lifecycle transitions, Modbus
retirement, and Gateway shutdown. Invalid values fail closed to `unknown`; no
endpoint, address, interface, SKI, serial, identity, credential path, source
locator, or error text enters the metric.

The exact-HEAD review corrections:

- fence eeBUS observer delivery with a Gateway-internal revision, so a delayed
  attach or old transition cannot overwrite a newer lifecycle tuple; a newer
  invalid tuple keeps its revision and truthfully replaces the older value as
  `unknown`;
- publish Modbus and eeBUS `retired` states before the metrics-serving control
  plane closes. eeBUS retirement atomically detaches and fences the metric
  observer while it publishes the final revision; previously captured lower
  revisions are rejected and live optional-admin recovery cannot publish a
  later metric. This does not cancel, join, retire, or shut down the native
  runtime: existing recovery and teardown remain their owners;
- derive the HTTP control-plane context with `context.WithoutCancel`, preserving
  request/operator context values while deferring shared lifecycle cancellation
  until after retirement publication. The ordinary teardown then performs a
  bounded `http.Server.Shutdown`, waiting for active requests to drain; forced
  `Close` is used only after a reported timeout/error. The listener no longer
  owns a competing cancellation goroutine;
- treat a composed public read-only eeBUS runtime as metric `ready` even when
  the optional typed operator-admin capability is unavailable. Admin recovery
  remains bounded and incomplete until that optional capability is available;
- derive the passive-smoke fixture's pre-candidate source only from local
  history: `HEAD^`, then local `main`. It no longer reads `origin/main` or any
  remote-tracking ref. A self-contained regression creates and removes an
  origin-style ref, proves the exact allowed classifier hunk works after its
  removal, and confirms a hostile passive-state hunk still fails closed.

Focused RED-first validation passed:

- `CGO_ENABLED=0 GOWORK=off go test -race . ./cmd/gateway -run 'Test(TransportRuntimeMetrics|EEBusTransportObserverFences|EEBusTransportRetirementFences|EEBusPublicRuntimeFallback|GatewayPublishesRetirement|ModbusRuntimeTransportStatus|EEBusRuntimeTransportStatus)' -count=1` — PASS (two packages) for the earlier source amendment.
- `python3 scripts/passive_smoke_gate_test.py` — PASS (13 tests) in the candidate checkout.
- `python3 scripts/transport_gate_test.py` — PASS (28 tests).
- An isolated detached clone at the exact candidate HEAD had its `origin`
  remote removed and no `refs/remotes/origin/*`; its classifier suite passed
  all 13 tests. `NO-ORIGIN-CLASSIFIER.log` SHA-256 is
  `5b84bde92a2a701bf7e3ca8a71802f5088851a4026a27d3ea6133d05b8144d41`,
  with exit artifact SHA-256
  `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`.
- `GOWORK=off ./scripts/passive_smoke_gate.sh` and
  `GOWORK=off ./scripts/transport_gate.sh` — both `not triggered`; no override
  used.
- The actual-listener regression starts `/metrics` on a loopback ephemeral
  port, cancels the shared lifecycle context, publishes both retirement tuples,
  reads those tuples over HTTP, then performs bounded graceful shutdown and
  proves the listener is closed. It also proves context values survive the
  intentional cancellation split.
- Additional listener regressions hold an admitted request open and prove
  graceful shutdown waits for its successful completion, then use an already
  canceled drain context to exercise immediate forced-close fallback without a
  sleep. The fallback returns the original graceful-shutdown error so operators
  can distinguish a clean drain from forced closure.

The first complete CGO-disabled run exited `1` only because the earlier
SemReg-only passive-smoke classifier did not recognize the detached Gateway
transport snapshot. The sanitized faithful log is `FULL-CI-LEAD.log` (SHA-256
`53d8c381b36e59be014729b8b649de669cb6814019568c6105dff9135983a9bd`), with
exit artifact SHA-256
`4355a46b19d348dc2f57c046f8ef63d4538ebb936000f3c9ee954a27460dd865`.

The first full run after adding the HTTP listener regression exited `1` at
`golangci-lint` because the new test did not check `response.Body.Close()`.
All preceding phases passed. The faithful sanitized failure is retained as
`FULL-CI-CLOSE-ERRCHECK-FAIL.log`, SHA-256
`e0b3b9453465807fd7c62e724c92396c2e24f60c903491e7f74e8cb8c2a1698c`;
its exit artifact SHA-256 is
`4355a46b19d348dc2f57c046f8ef63d4538ebb936000f3c9ee954a27460dd865`.
The test now reports read and close failures independently.

The first full run after the graceful-drain correction exposed two stale
source-order tests that still required `server.Close()` and one timing failure
in the pre-existing B503 read-lock deadline leaf. The order assertions were
updated to require `shutdownHTTPControlPlane(server)`; the B503 leaf then passed
20/20 under `-race`, and the subsequent complete suite passed it as well.

The complete `CGO_ENABLED=0 GOWORK=off ./scripts/ci_local.sh` run before the
latest admin-lock P1 correction exited
`0`. Its sanitized faithful log is `FULL-CI-977.log` (336 lines, SHA-256
`ec40d47e3e5928dd3bed9a95a6641b09e9a70d0af0170e63ef5e384e2ec27629`) and the
exact exit artifact has SHA-256
`9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`.
It passed 158 Portal tests; the global Go race suite; Python suites of 168, 6,
28, 13, 6, and 2 tests; `golangci-lint`; both semantic mapping gates; and the
transport and passive-smoke gates (`not triggered`). Before committing, a search
across author evidence found no account or private workspace path; captured
path portions are replaced with `[workspace]` without changing command, result,
or gate semantics.

The ordinary macOS `GOWORK=off ./scripts/ci_local.sh` build fails before tests
because the installed Command Line Tools linker rejects SDK `.tbd` entries for
`arm64e.x1`. The CGO-disabled run is the local reproducible validation path and
does not alter repository code or transport behavior.

Docs gate: satisfied by `docs/transport-prometheus-runtime-v1.md`. T01..T88
and P01..P06 are not applicable: this candidate adds no eBUS framing, topology,
passive-smoke, or Modbus RTU transport behavior.

Latest P1 closure: `eebusRuntimeLifecycle.ServeHTTP` now snapshots its handler
under `RLock` and releases the lifecycle lock before invoking a potentially
blocking admin request. `TestEEBusAdminRequestDoesNotBlockTransportRetirement`
keeps an admin request blocked, proves retirement completes first, then releases
the request. This retains both bounded HTTP drain and terminal retirement.

Final P1 validation: `CGO_ENABLED=0 GOWORK=off ./scripts/ci_local.sh` exited
`0`; `FULL-CI-977.log` is 336 lines with SHA-256
`6f353a367d9f98f32d43d2c38feae25411aea7b4b6593557e0c3ea120518c043` and its
exit artifact SHA-256 is
`9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`.
It passed the global Go race suite, 158 Portal tests, Python suites, lint,
semantic mapping gates, and transport/passive-smoke gates. Evidence was
sanitized and contains no account or private workspace path.

Hosted CI run `34986010184` exposed exactly two failures in
`passive_smoke_gate_test.py` in a shallow checkout: neither `HEAD^` nor
local `main` existed, so the history-only fixture lookup failed. The test now
uses a checked-in pre-candidate fixture only after those local refs are absent.
Its depth-1 regression detaches HEAD, deletes local `main`, verifies both
lookups fail, and proves fixture fallback; the suite reports 14 passing tests.

Final exact-tree validation: `CGO_ENABLED=0 GOWORK=off ./scripts/ci_local.sh`
exited `0`; `FULL-CI-977.log` has 336 lines and SHA-256
`1722807b4c175e10f72dfb0dd410d23807d61286a6ac3dbec37d01f39e7ef13e`. It passed 158 Portal tests, the global Go race suite, Python
suites of 168, 6, 28, 14, 6, and 2 tests, `golangci-lint` with 0 issues,
both mapping gates, and transport/passive-smoke gates not triggered.

Latest shallow-history P1 closure: classifier tests now always use the immutable
checked-in pre-candidate fixture, rather than moving `HEAD^` or `main`.
The successor regression commits an unchanged successor and verifies the exact
allowed snapshot hunk still passes while a hostile passive-state mutation fails.
Final `CGO_ENABLED=0 GOWORK=off ./scripts/ci_local.sh` exit was `0`;
336-line `FULL-CI-977.log` SHA-256: `fa630596151685d2fd02ffcb5fac2eb9a205f602d5b5dc87da2a70e50771dfa8`. It passed Portal 158,
global Go race, Python 168/6/28/14/6/2, lint 0, mapping gates, and transport /
passive gates not triggered.

## Terminal retirement correction

The live PR feedback identified a teardown race: a recovery attempt that returned
a valid runtime after `PublishTransportRetirement` could replace the retired slot
and restore the admin handler even though the final metric had already been
published. The corrected lifecycle uses one short publication linearization
section for candidate adoption versus terminal retirement. Native start,
shutdown, handler execution, and observer callbacks all run outside that section.
Retirement records the final state and revision, clears handler/admin/runtime
availability, detaches the observer, and applies an immediate slot fence. A
starter result returned after that point is cleanup-only and its runtime is shut
down exactly once.

The runtime slot now separates pointer transitions from native shutdown. Its
terminal fence rejects new reads and replacements without waiting for an
in-flight native read. Detached old generations drain outside slot and lifecycle
locks, and final shutdown waits for all detached drains before returning their
errors. Recovery state helpers reject all state and revision changes after the
terminal fence.

Focused validation:

- `CGO_ENABLED=0 GOWORK=off go test ./cmd/gateway -run 'TestEEBusTransportRetirement|TestEEBusAdminRequestDoesNotBlockTransportRetirement|TestIssue846Lifecycle|TestIssue846RuntimeSlot|TestEEBusRuntimeSlotTerminalFence' -count=1 -timeout=60s` — PASS.
- The same selection with `go test -race` and a 120-second timeout — PASS.
- `CGO_ENABLED=0 GOWORK=off go test ./cmd/gateway -count=1 -timeout=180s` — PASS in 89.410 seconds.
- The new in-flight-start regression blocks the second native start, publishes
  retirement first, then returns a valid runtime/admin pair. It proves the
  terminal snapshot and revision remain unchanged, the candidate runtime is
  shut down once, admin and runtime access remain unavailable, recovery exits,
  and the final metric remains retired.
- The terminal-slot regression holds a native reader open and proves the fence
  returns without waiting or calling native shutdown, rejects a new reader, and
  leaves final shutdown as the sole native owner.

The first complete post-correction CI run exited `1` on the pre-existing
`TestIssue552B503DisableReadMuDeadlineBeforeEmissionPreservesOwner` timing leaf;
all changed lifecycle tests passed. That leaf then passed 10/10 under `-race`.
The faithful sanitized failure log is
`FULL-CI-P1-TERMINAL-FENCE.log` (4,472 lines, SHA-256
`c3e41dc1fbc8cf69fb5acdd8c66a358f0ea1c3f746fde77d27a10893a49773b1`), and its
exit artifact SHA-256 is
`4355a46b19d348dc2f57c046f8ef63d4538ebb936000f3c9ee954a27460dd865`.

The required complete rerun of
`CGO_ENABLED=0 GOWORK=off ./scripts/ci_local.sh` exited `0`.
`FULL-CI-P1-TERMINAL-FENCE-RERUN.log` is 336 lines with SHA-256
`4727054c3b4422403d246a6ae4ecd53b221cf1b2154db3b981b3204584b57888`;
the exit artifact SHA-256 is
`9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3ab86aa`.
It passed 158 Portal tests, the full Go race suite, Python suites of 168, 6,
28, 14, 6, and 2 tests, `golangci-lint` with zero findings, both semantic
mapping gates, and transport/passive-smoke gates (`not triggered`). No override
was used. T01..T88 and P01..P06 remain outside this metrics-only change.

## HTTP admission-order correction

A later live review found that final retirement publication preceded several
native cleanup calls but HTTP shutdown followed them. If one cleanup stalled,
the listener could admit new GraphQL, MCP, Portal, or Modbus requests after the
public transport metric already reported `retired`. The teardown now publishes
the final bounded statuses and immediately starts bounded HTTP shutdown in the
same first LIFO defer. `http.Server.Shutdown` stops new admission before waiting
for already-admitted requests; only after that bounded drain completes do the
existing broadcast, deduplication, reconstruction, observability, and mDNS
cleanup steps run.

A source-order regression proves retirement and HTTP admission stop share the
first teardown defer, with publication first, and that every later cleanup is
registered in the earlier defer. The focused control-plane and metrics tests
passed under `-race`.

The exact post-correction
`CGO_ENABLED=0 GOWORK=off ./scripts/ci_local.sh` run exited `0`.
`FULL-CI-P2-ADMISSION-RERUN.log` is 340 lines with SHA-256
`b6dbc915adb0a147769a8193fae7c6c7235e083e86e152a2d798b920d774789f`;
the exit artifact SHA-256 is
`9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3ab86aa`.
It passed 158 Portal tests, the complete Go race suite, Python suites of 168, 6,
28, 14, 6, and 2 tests, and lint with zero findings. Because the teardown source
changed, the repository transport gate conservatively triggered and passed the
Modbus RTU production composition and pinned endpoint conformance checks. Both
SemReg mapping gates passed; passive smoke remained not triggered. No override
was used. T01..T88 and P01..P06 remain outside this change.
