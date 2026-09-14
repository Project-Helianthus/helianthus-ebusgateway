# Issue 552 B503 transport-gate correction

## Scope

This correction makes the eBUS transport-gate classifier explicitly cover the
native B503 operation surfaces introduced by the Issue 552 change:

- `cmd/gateway/vaillant_b503_dispatcher.go` — native raw-frame dispatch;
- `cmd/gateway/vaillant_b503_wiring.go` — production dispatcher installation;
- `internal/vaillant/b503session/*.go` — session ownership and operation fences.

These files can select, fence, or emit native eBUS operations.  A non-test
change to any of them now requires the current M6a T01..T88 matrix report.
Tests alone remain non-triggering, consistent with the gate's existing
source-only classification policy.

## Regression evidence

Command:

```sh
python3 scripts/transport_gate_test.py
```

Result: `PASS` — 27 tests.  The added regression changes each representative
B503 native source in a temporary Git repository and intentionally omits
`TRANSPORT_MATRIX_REPORT`.  It asserts the fail-closed error
`TRANSPORT_MATRIX_REPORT is required`, which proves the B503 classifier fired.

Command:

```sh
SDKROOT="$(xcrun --sdk macosx --show-sdk-path)" \
GOWORK=off go test -race -tags=raceconcurrency ./cmd/gateway \
  -run '^TestM6(Conc|Dispatcher_)' -count=1
```

Result: `PASS` — the non-live B503 concurrency/production-dispatcher bridge
suite passed under the race detector.  It includes the four `TestM6Conc*`
lock-order and reconnect scenarios and the `TestM6Dispatcher_*` native
dispatcher/wiring coverage selected by the expression.

## Matrix-gate state

Command:

```sh
env -u TRANSPORT_MATRIX_REPORT -u TRANSPORT_GATE_OWNER_OVERRIDE \
  -u TRANSPORT_GATE_OWNER_REASON TRANSPORT_GATE_BASE_REF=main \
  bash scripts/transport_gate.sh
```

Result: expected non-zero exit.  The gate reports
`TRANSPORT_MATRIX_REPORT is required for eBUS transport/protocol changes.`
No report was fabricated or reused, no owner override was supplied, and no
live hardware was contacted.  The remaining merge blocker is therefore a
legitimate current 88-case T01..T88 matrix report for this exact PR HEAD.

## Delivery boundary

The requested route was Terra/high.  This subtask runtime does not expose
independent per-agent model/effort metadata, so the recorded route is the
delegated assignment rather than a separately observable runtime assertion.
The correction is committed and pushed as a reviewable checkpoint. The
required matrix gate remains a merge blocker because a legitimate current
report is not available.
