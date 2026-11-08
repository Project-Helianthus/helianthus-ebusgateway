# Managed sidecar composition after G5

The gateway composes SunSpec TCP, Growatt BMS RS-485, the receive-only Growatt
BMS CAN observer and eeBUS through the shared runtime registry, native manager
and durable lifecycle service.
Physical family activation remains Unknown.

## Gateway registration and consumer behavior

Proven: the gateway composes these sidecar families through the shared registry,
native manager and durable service. Disabled entries remain visible without constructing native
runtimes. Consumer bindings acquire only the current admitted generation.

The managed eeBUS shell intentionally registers its read and command-router
MCP bindings even when eeBUS is disabled, stopped or unavailable. This changes the
previous eager-start behavior, which omitted these tools without a started
runtime. Discovery does not grant native admission: live reads return
`backend_unavailable` and the command router returns `disconnected` while no
generation is admitted. Existing request authorization, trust and mutation
safeguards still apply. The disabled-case M06 fixture pins the complete eeBUS
public list of nine read/evidence tools, excludes the five operator-only raw
command tools from that list, and checks unavailable reads, mutations and
rollback without native construction. The managed GraphQL compatibility
overlay is composed but empty without admitted observations. When explicitly enabled, synchronized one-shot
evidence also retains the managed provider and router; registration alone does
not establish evidence availability. Portal readiness reads the managed
lifecycle snapshot rather than a retained eager runtime.

`prepareManagedSidecars` registers `ebus.primary` before assembling the shared
manager. `composeEBusPrimary` accepts that same descriptor when identical;
conflicting registrations still fail. The retained Tesla HSC owner starts after
the runtime control service, and its shutdown remains in reverse acquisition
order. Native sidecar work and consumer leases belong to their managed
generations rather than duplicate eager shutdown calls.

Synchronized evidence and leaf-promotion capture retain shell-facing bindings,
not native generation pointers. Each capture resolves the currently admitted
eeBUS generation and fails closed while none is admitted. Synchronized evidence
configuration remains one-shot; later START and RESTART need no reconfiguration.
Leaf-promotion checkpoint-store construction remains lazy. Proven: construction
errors and nil constructor results fail closed with `ACQUISITION_FAILED` and
log a diagnostic identifying the construction failure.

The offline Modbus RTU gate includes the `TestM06` composition fixtures alongside
`TestM04` and the existing gateway fixtures, followed by the native
`TestRTUProduction` conformance tests. Gate selection is independent of results.
Unknown: physical transport behavior and installation qualification; software
fixtures do not establish either.

### Sidecar start failures

A sidecar START whose manager attempt does not reach `RUNNING` returns a
closed executor error carrying the manager's reason, so the lifecycle view
reports `PROVIDER_UNAVAILABLE` (or `CONFIG_INVALID` for an eeBUS or CAN
configuration category) rather than `INTERNAL_ERROR`. The error carries no
native text. SunSpec and Growatt BMS RS-485 keep non-retryable start errors
and the service policy. The gateway re-issues a failed Modbus START as an
ordinary RESTART decision (see "Modbus start retry and failed-start succession"
below). A Growatt BMS RS-485 START that fails because its configuration is
incomplete or its RS-485 timing is rejected reports `CONFIG_INVALID`. Every
other Growatt BMS RS-485 and Modbus TCP start failure reports
`PROVIDER_UNAVAILABLE`.

eeBUS classifies a native start error with the construction categories, so
a protected-credentials failure reports `LOCAL_IDENTITY_UNAVAILABLE` and only
listener errors report `LISTENER_UNAVAILABLE`. Each failed eeBUS start logs
exactly one line, `eeBUS runtime unavailable; continuing without eeBUS`, with
the stage (`operator`, `read_only` or `read_only_fallback`) and the closed
categories only. The eeBUS sidecar keeps a zero manager retry budget; its
service `RetryPolicy` allows three start attempts five seconds apart, the
window of the earlier eeBUS lifecycle, so the service is its only retry
owner. A retry is a later generation of the same epoch. A failed START admits
nothing, and the successor first retires the failed native attempt (see
"Modbus start retry and failed-start succession" below). The `TestM69`
fixtures in
[`eebus_start_failure_visibility_test.go`](../../cmd/gateway/eebus_start_failure_visibility_test.go)
drive the production startup with the add-on's eeBUS flag set and injected
native factories. They do not establish the native cause on any
installation.

### Receive-only Growatt BMS CAN

Proven: `growatt.bms.can` is registered through the shared manager and durable
lifecycle service. It is disabled by default, with no constructed receiver.
The host enables it with `-growatt-bms-can-enabled` and supplies
`-growatt-bms-can-interface`, `-growatt-bms-can-listen-only-confirmed` and a
positive `-growatt-bms-can-stale-after` duration. These options appear in
gateway help. The controller must already be in listen-only mode before the
receiver opens. The confirmation records that precondition; the observer does
not verify or configure controller mode and never transmits.

Construction is inert; START validates configuration and opens a Linux
SocketCAN receiver. Invalid configuration, missing listen-only confirmation
and unsupported platforms fail closed without retry. Retryable start failures
use the service policy of up to three attempts, with a five-second delay
before each retry. Two retries contribute ten seconds of backoff, plus attempt
time, within the existing service budget. The manager retry budget is zero,
so this does not create a second retry owner.

After admission, receive-loop termination withdraws effective capabilities and
publishes `FAILED/PROVIDER_UNAVAILABLE` through the manager and runtime control
service. Notifications retain native and lifecycle generation correlation;
an early failure waits for START completion, and a retired receiver cannot
fail its successor. Automatic retries cover startup; recovery from an admitted
receive-loop failure requires explicit RESTART. STOP joins reception before
closing the descriptor, and successful retirement precedes a replacement
receiver. Unconfirmed receiver cleanup latches native safety quarantine and
prevents opening a replacement.

CAN configuration is host-only. It is absent from the closed persisted Portal
driver-selection record and has no Portal selection control. A corrupt,
unsupported or locked Portal record does not withhold valid host-enabled CAN;
the record-governed drivers still fail closed. This exemption applies only to
Portal selection: durable lifecycle state and recovery requirements still
apply to CAN.

`RUNNING` establishes lifecycle admission only. Public CAN snapshot exposure
through MCP or GraphQL remains deferred; no observation tool, semantic
publication or native control operation is added. The `TestCAN03` fixtures in
[`growatt_bms_can_cli_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/growatt_bms_can_cli_test.go)
cover configuration, retries, receive failures, restart and cleanup. The
`TestM28` failure-isolation fixtures in
[`portal_config_runtime_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/portal_config_runtime_test.go)
keep CAN running while preserving Portal rejection and governed-driver
withholding. These are synthetic software checks. Physical qualification and
live trials are not established; installation behavior remains Unknown.

### Modbus TCP driver instances

The [multi-endpoint contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/modbus-multi-endpoint-runtime.md)
governs this composition. Configuration v1 (`-modbus-tcp-endpoint` or the
single-line `-modbus-tcp-endpoint-file`) still composes exactly one
`modbus.sunspec` descriptor with its source, durable record, transport limits
and log lines unchanged. Configuration v2, the owner-only document read
through `-modbus-tcp-endpoints-file`, composes one descriptor per entry. Its
driver ID is the family base, plus a dot and the label unless the label is
`default`. Each descriptor owns exactly one source, derived from that driver ID
and the instance GUID as before. The Modbus TCP set registers as one batch:
a duplicate or a source collision refuses only the set, with no factory call.

Each instance has its own connection, worker, unit, cycle, generation,
durable record and retry budget, so a failure, STOP or RESTART of one instance
leaves every other instance and eBUS, eeBUS, HTTP and MCP untouched. A v2
instance holds one connection with one request in flight. Every request of
the instance, including retries and on-demand reads, passes one gate that
enforces the gap after the previous completion and the settle wait after each
connect; a wait never extends a read or attempt bound. SmartLogger and
S-Dongle instances compose their admitted native read-only providers. EMMA
registers withheld (`NO_ADMITTED_CHILD_PROFILE`): it lists as FAILED with
`PROVIDER_UNAVAILABLE`, logs one categorical line and constructs, dials and
publishes nothing. Raw reads, retained observations, the PV projection and its
Portal panel keep addressing the v1 slot only: the v1 endpoint, or the v2
`sunspec` entry labelled `default`. Logs carry driver IDs, never endpoints.
The `TestH08` fixtures in `cmd/gateway` and `internal/modbusadapter` cover
this offline; they qualify no installation.

### Modbus start retry and failed-start succession

A supervisor in the gateway shell re-issues the failed START of a managed
Modbus driver. It supervises every composed Modbus TCP instance (the v1
`modbus.sunspec` descriptor and each v2 instance) and `growatt.bms.rs485`.
eeBUS keeps its own service policy. A disabled driver, an uncomposed driver and
a withheld driver (including a Huawei instance withheld as
`NO_ADMITTED_CHILD_PROFILE`) are not supervised.

A failed START of a supervised driver that is desired `RUNNING` schedules one
retry when its reason is `PROVIDER_UNAVAILABLE`, `DEPENDENCY_UNAVAILABLE`,
`RUNTIME_NOT_READY` or `RETRY_EXHAUSTED` and the driver is not quarantined. The
retry is an ordinary durable `RESTART` decision against the failed desired
revision, fenced by that expected revision. The sidecar's start errors stay
non-retryable at the service, so each retry is one START attempt under a new
decision with its own budget. The backoff, the fencing, what ends a retry,
the runtime-control view and the log line are defined in the
[runtime/provider contract](runtime-driver-provider-contract-v1.md#automatic-start-retry-of-managed-modbus-drivers).
`CONFIG_INVALID`, `INTERNAL_ERROR`, `CLOSE_UNCONFIRMED` and a quarantine are
never retried. `RETRY_EXHAUSTED` caused by `CLEANUP_STALLED` is also excluded:
unresolved semantic cleanup blocks recovery admission. A terminal decision
refusal ends the scheduled retry rather than starting another backoff.

**Failed-start succession.** The executor records the lifecycle key of its
START. A START under a different key succeeds a failed START only when all of
the following hold:

- the recorded START has completed with a failure;
- the driver and the source are the same;
- the new key is a later generation of the same epoch (a service retry, which
  keeps its decision sequence), or it belongs to a different epoch with a later
  decision sequence (a RESTART, including the supervisor's).

Every other key is refused as stale. A key of the same epoch with a different
decision sequence is also stale.

Before the successor binds the lifecycle, the executor retires the failed
native attempt. It runs the managed runtime's stop, which fences withdrawal,
drains the consumer leases and stops the native owner. A failed START can retain
a native attempt even when no endpoint opened.

| Retirement | Result |
| --- | --- |
| Proven: the stop returns no error and the native runtime is not safety-quarantined | The predecessor's generation is recorded as retired, so a START under it is refused as stale and a STOP for it reports the close as done. The failed START record is cleared. The successor binds its lifecycle and starts. |
| Unproven: the stop returns an error, or the native runtime reports the safety quarantine | The START fails with a non-retryable start error that names `CLOSE_UNCONFIRMED` and carries no native text. The lifecycle is not rebound. The recorded key and the failed START record stay as they were, the predecessor is not recorded as retired, the native runtime is not started and no successor is admitted. The supervisor does not retry it. |

The control service preserves this start-error reason and publishes
`FAILED/CLOSE_UNCONFIRMED` with `SafetyQuarantined` set. It refuses subsequent
operator RESTART decisions as quarantined until process restart, even if cleanup
of the refused successor's own correlation reports a proven close. An
unconfirmed-close start error is never retried, even if marked retryable.
Quarantine blocks admission while the existing transition continues its bounded
cleanup and persistence retries. Proven closure of its own correlation permits
semantic cleanup retries and terminal slot pruning without clearing quarantine;
an unconfirmed cleanup close retains its obligation without automatic retry.

Offline fixtures in `cmd/gateway` cover the backoff, the retryable reasons,
the supervised set, the cancellation conditions, the succession keys and the
refused succession; they qualify no installation.

## Contract precedence and evidence

The public [runtime driver lifecycle contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
owns G5 decisions D1, D2, D3 and D5. They are reused unchanged. The local
[runtime/provider contract](runtime-driver-provider-contract-v1.md) records the
implemented seams; its S1/D3 amendment supersedes historical receipt and
idempotency-record passages. The
[bidirectional registration proposal](bidirectional-driver-registration.md)
does not make reserved hooks available.

Native protocol meaning remains with the public
[Modbus registry](https://github.com/Project-Helianthus/helianthus-modbusreg),
[Modbus transport](https://github.com/Project-Helianthus/helianthus-modbus), and
[eeBUS registry](https://github.com/Project-Helianthus/helianthus-eebusreg)
contracts. Native qualification, transport admission, SHIP trust, SPINE
interpretation and raw evidence stay there. New protocol facts require their
public native documentation contract first. This document introduces none.
Protocol-neutral publication remains owned by
[SemReg](https://github.com/Project-Helianthus/helianthus-semreg).

**Proven** below means an inspected software seam exists; it does not qualify
an installation. **Hypothesis** describes a proposed behavior awaiting
implementation evidence.
**Unknown** means evidence or an implementation decision is absent. Candidate,
qualified, promoted, unsupported and unknown native states remain distinct
from these evidence labels and from runtime admission.

## Existing compiled seam

The compiled registry lives in
[`internal/runtimecontrol/types.go`](../../internal/runtimecontrol/types.go),
[`registry.go`](../../internal/runtimecontrol/registry.go) and
[`provider.go`](../../internal/runtimecontrol/provider.go):

| Existing input or result | Meaning at the composition boundary |
| --- | --- |
| `Descriptor` | `Format`, canonical `DriverID`, `Direction`, configured `Disposition`, `NativeContract`, `Adapter`, owned `Sources`, `Hooks`. Use the existing descriptor format; add no plugin loader. |
| `Registry.Register` | Validates the whole batch, including disabled entries, before installing frozen copies. Duplicate drivers/sources and conflicting hub ownership refuse the batch without factory calls or hub mutation. |
| `Registry.List`, `Get`, `Binding` | Detached coherent reads; list order is bytewise by driver ID. `Binding` returns the stored adapter and frozen construction material without opening anything. |
| `Construction` | Exactly `DriverID`, `Direction`, `NativeContract`, `Sources`. It contains no epoch, generation, endpoint configuration or authority. |
| `LifecycleAdapter.Construct(ctx, construction)` | Returns `drivermanager.Runtime` or error. `NewAdapter[R]` wraps a typed factory and rejects a nil runtime, including typed nil. |
| `Hooks`, `ResolveHook` | All reserved hooks are unimplemented; unknown hooks also fail. Leave hooks empty. Existing MCP/Portal/semantic wiring is explicit application composition. |

The existing production example is `composeEBusPrimary` in
[`gateway_run_setup.go`](../../cmd/gateway/gateway_run_setup.go). It binds
manager hooks before startup and accepts the identical `ebus.primary`
descriptor registered by `prepareManagedSidecars`. Its typed factory returns
the already prepared native runtime wrapper; registry acceptance alone does
not connect the service to a family. The gateway explicitly composes the
family bindings described above; reserved hooks remain unavailable.

## Adapter inputs, outputs and ownership

M02–M06 must attach each family through the existing service, executor and
DriverManager boundaries, rather than introduce another driver manager.
The following are required application bindings, not new exported Go APIs.

| Boundary | Concrete input | Required output and owner |
| --- | --- | --- |
| Composition root | Validated family configuration, configured driver/source identities, exact public native contract reference, injected native factory/open/dial functions, hub, durable store and recovery adapter | Frozen descriptor, constructed wrapper installed as `drivermanager.DriverConfig.Runtime`, and one `runtimecontrol.DriverConfig{DriverID, Executor}` in the same service, in the order below. Configuration and factory dependencies are captured by the family adapter, not encoded in public IDs. Missing identity/dependency withholds that driver. |
| Typed construction | `context.Context`, frozen `Construction` | Family wrapper implementing existing `drivermanager.Runtime`: `Start(ctx) (uint64, error)`, `Replace(ctx) (uint64, error)`, `Stop(ctx) error`, `Generation()`, `Revision()`, `SafetyQuarantined()`. Construction returns an inert wrapper; native opening belongs to its managed start. Clean up partial factory results on failure. |
| Lifecycle execution | `Start(ctx, LifecycleCorrelation)`; correlation binds driver, configured source, epoch, driver generation and lifecycle sequence | Nil only after that generation is admitted. Failure admits nothing; a native attempt it retained is retired before a successor binds (see "Modbus start retry and failed-start succession"). Maintain an explicit mapping to native generation; do not cast native counters into semantic generation. Repeated calls for one key join or return its result, without a second start. |
| Withdrawal | `Stop(ctx, LifecycleCorrelation)`, including a generation that never opened | `StopOutcome{NativeClosed, SemanticCleaned}` with independent, truthful obligations. Native close proof comes from `StopWithReport`; semantic cleanup comes from fenced publication withdrawal/epoch retirement. A close error cannot be converted to success because cancellation was sent. |
| Recovery | Existing `RecoveryAdapter.Restore`, `Secure`, and managed `Recorder.Writer(source)` when semantic publication is migrated | Restore before native open/admission; commit secured recovery material before pruning terminal work. Route managed publications through the recorder, not a parallel direct writer. A lifecycle-only adapter cannot claim recovery for semantic assets. |
| Consumer binding | Current admitted generation plus family-owned provider interfaces, Portal contribution leases and publication handles | Stable shell-facing indirection; unavailable while withdrawn. Install generation effects only through a live generation scope and revoke leases before close. Never leave a request holding an unguarded retired provider pointer. |

Gateway composition order for M02–M06:

1. Validate and register the complete descriptor batch before invoking any
   adapter. For each enabled, non-withheld driver, obtain
   `binding, err := registry.Binding(driverID)` (handling its error), then call
   `binding.Adapter.Construct(ctx, binding.Construction)` once to obtain its
   inert wrapper. Neither registry acceptance nor service registration calls
   `Construct` on the application's behalf.
2. Assemble the complete initial `[]drivermanager.DriverConfig`, including the
   existing primary driver and each successfully constructed sidecar. Each
   sidecar entry has `ID: string(driverID)`, `Enabled: true`, `Runtime: wrapper`,
   and its family-owned capabilities, error classifier and retry policy. Pass
   the complete list to `drivermanager.New` for the shared manager before
   creating lifecycle bridges/executors or the service. There is no later
   runtime registration API. `newEBusDriverController` constructs this shared
   manager with the complete initial list; no second manager or later append
   is needed.
3. Bind each family bridge and its admission/close hooks to that same manager
   and driver ID before any manager start. Create its executor and install
   `runtimecontrol.DriverConfig{DriverID, Executor}` in the shared service.
   That service config carries the executor, not the constructed runtime.
4. Bootstrap configured durable entries, create the service with recovery,
   then start it: restore before reconciliation may open any native runtime.
   Disabled entries remain visible with `Enabled: false` and no constructed
   runtime; do not call their adapters. A binding/construction failure withholds
   the affected manager runtime and supplies a failing executor for its
   configured service entry, preserving shell and other-driver operation.
   Clean up partial construction on failure; never start a partial manager
   configuration after manager creation fails.

`LifecycleCorrelation` supplies identity, not configuration or native authority.
The service allocates epochs. The family binding supplies the configured source
and maps correlation to the native generation. DriverManager remains the native
retry owner; family reconnect stays native-owned. Executor errors must not
create an additional service retry loop around a manager-owned retry budget.
The current service leaves `SourceID` unset in its correlations; the eBUS
bridge binds its configured source and rejects a conflicting supplied source.
Sidecar bindings must complete that key explicitly, without deriving identity
from a connection. Lifecycle sequence 0 is permitted for the untyped
process-start attempt; epoch and native mapping still distinguish it.

## Existing sidecars and required attachment

These methods are implementation anchors, not evidence of completed managed
composition. Keep the current family-specific decoders and evidence paths.

| Family | Existing inputs and returned surface | Required managed attachment |
| --- | --- | --- |
| SunSpec TCP | [`startModbusRuntime`](../../cmd/gateway/modbus_runtime_adapter.go) takes context, `ModbusTCPConfig`, hub, `modbusadapter.Dialer`, `modbusadapter.Factory`; returns `*modbusadapter.Adapter` or error. `modbusadapter.Start` opens the native endpoint. `Adapter.Close` closes it and retires its PV publication. | Inject the existing factory and dialer into the wrapper. Move the opening call under managed start. Own the qualify/refresh worker and Portal PV lease per admitted generation; join the worker and revoke the lease before adapter close. Migrate the existing direct/legacy publication ownership to the managed writer and epoch barrier before claiming D5 recovery. |
| Growatt BMS RS-485 | [`startGrowattBMSRS485Runtime`](../../cmd/gateway/growatt_bms_rs485_runtime.go) takes `GrowattBMSRS485Config`, hub; returns `*growattBMSRS485ProductionProvider`. `openGrowattBMSRTUEndpoint` is the existing injectable open seam. Provider methods include `GrowattBMSRS485V202`, `GrowattStorageSemanticCurrent`, `LastObservationEvidence`, `Close`. | Bind config/source and service correlation before opening. Preserve the existing admission-before-endpoint ordering, native read admission and observation serialization. Add cancellable, bounded join around in-flight reads; `Close()` has no context and cannot alone prove a bounded drain. Keep immutable native observation evidence and retained storage fields. Attach the Storage contribution to the actual admitted generation. |
| Growatt BMS CAN | [`newGrowattBMSCANManagedFactory`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/growatt_bms_can_runtime.go) takes `GrowattBMSCANConfig` and an injectable receive-only opener; returns an inert lifecycle adapter. | Proven: host-only registration, disabled defaults, managed start/stop/restart, bounded startup retries, correlated receive-failure reporting and cleanup quarantine. Public snapshot bindings remain deferred. |
| eeBUS | [`startEEBusRuntime`](../../cmd/gateway/eebus_runtime_adapter.go) takes context, `EEBusConfig`, interface resolver and `eebusRuntimeFactory`; returns `*eebusRuntimeAdapter`. The operator variant returns adapter plus `AdminV1`. Native lifecycle is `Start(ctx)` / `Shutdown()`; adapter provides `Snapshot()` and once-only `Shutdown()`. | Construct and start inside managed ownership. Preserve incomplete-pair cleanup in `startEEBusOperatorRuntime`. Use the existing `eebusRuntimeSlot` reader drain/replace shape and command router; fence readers before draining. Its `Shutdown()` has no context, so prove bounded close/join or report unconfirmed close. Do not turn an eeBUS trust or native write decision into a generic lifecycle decision. |

The existing `eebusRuntimeSlot` methods `BeginRetire`, `DrainRetired`,
`ReplaceDetached`, `shutdownDetached`, `FenceTerminal` and `Shutdown` separate
availability fencing from reader drain and native teardown. They are
eeBUS-local primitives, not a second runtime control service. A detached old
runtime must be shut down and its drain accounted for. A rejected replacement
must also be closed. A slot becoming unavailable proves neither native close
nor semantic cleanup.

## Growatt BMS RS-485 lifecycle implementation status

Proven: `newGrowattManagedDriverFactory` constructs an inert wrapper around the
existing read-only BMS RS-485 family provider. It refuses unsupported profiles;
construction opens no endpoint. Start validates configuration, admits the source
before opening, and exposes the provider only after successful construction.
Disabled or failed starts expose no provider. This wrapper alone does not install
a descriptor, executor or driver in the shared service, migrate publication to
the managed recorder, or establish durable recovery or consumer rebinding.

Stop cancels the generation and removes the exposed provider immediately. Its
tracked drain fences semantic admission before waiting for in-flight acquisition
or native close. Retained Storage becomes unavailable when that fence completes,
including when a refresh stalls after a successful publication. Fencing runs
outside the lifecycle mutex, so a stalled journal or reprojection drain does not
block the caller deadline or provider and status inspection. A deadline does not
prove that semantic withdrawal completed.
Native evidence remains retained; a separate evidence lock lets the fence read
the last completed observation without waiting for transport acquisition.
`FenceWithdrawal` only cancels acquisition and publication admission. It takes no
publication lock and returns immediately while the manager holds its state lock.
Native evidence and semantic publication each enter an atomic commit admission
ordered against withdrawal. Withdrawal closes that admission without waiting;
Stop joins an already-entered commit before sampling fence evidence. A refresh
paused after observation cannot publish after withdrawal returns. Stop's tracked
drain awaits an already-entered publication's completion signal outside both the
driver mutex and manager lock, bounded by the generation's stop deadline. If
publication is still running at that deadline, Stop reports a publication fence
timeout and
quarantines the generation: fencing is not complete. Status reads and Replace
deadlines remain responsive. Background cleanup retains ownership of the stalled
publication; its eventual completion cannot clear the recorded fence timeout.
Cancelled or late refresh results cannot publish. Stop joins acquisition before
closing the endpoint once. A caller deadline returns its context error while
the drain continues; Start refuses a successor until that drain completes.
Close or fence failure quarantines the wrapper and refuses restart. Replace
requires successful Stop before Start; a successful restart advances the driver
generation and rejects calls through the retired provider.
The successor generation of the configured epoch, after Replace or Stop then
Start, publishes Storage again under its own binding, identity link, service and
facts. The epoch's source descriptor is upserted only by the epoch's first
publication, so it keeps that start and revision. A retired generation's
evidence stays refused through its own handle and through the successor's. The
`TestM61` fixtures in
[`growatt_managed_driver_test.go`](../../cmd/gateway/growatt_managed_driver_test.go)
prove this over register fakes that share one advancing receipt clock, reading
through the shared snapshot provider.

A stop during construction cancels and joins the pending start. If endpoint open
returns late, startup rechecks cancellation under the same lifecycle lock used
by Stop before exposing the provider. A failed or cancelled start withdraws only
an unused initial admission. An activated replacement is fenced and its handle
and generation retained for the next retry, even if no generation has published.
Cleanup precedes waiting for native close and settling.
A stop deadline does not prove that open, acquisition or close
finished.
Unknown: physical-device behavior; the deterministic injected-endpoint tests
exercise software cancellation, retained-state withdrawal, late results, timing,
admission and close failure without qualifying an installation.

The offline Modbus RTU transport gate classifies the managed wrapper and runs
all `TestM04` fixtures alongside the existing composition and native endpoint
conformance tests. These fixtures do not establish shared-service activation.

## Lifecycle rules reused unchanged

| Rule | Existing API and behavior | Sidecar obligation |
| --- | --- | --- |
| D1: new epochs | `runtimecontrol.Service` reconciliation allocates a new epoch on process start, explicit RESTART and START after completed STOP; driver generation begins at 1 and increases on retry. `ebusControlRuntime.Prepare`, `Start`, `Stop` demonstrate inert admission and succession. | Admit/restore before opening; bind the full key and native-generation mapping. Use hub `AdmitSuccessor` only after predecessor close and retirement. Never reuse a configured legacy epoch for a managed successor. |
| D2: shell-up, driver-failed, explicit repair | `runtimestate.Manager.BootstrapControl`, `ControlLoad`, service cached views and `startGatewayRuntimeControl` preserve failed-load disposition without overwriting control state. | A corrupt, unreadable, unsupported or locked store keeps affected drivers failed and unopened. Keep HTTP, health, MCP, GraphQL and status available. Use the existing explicit offline repair process; never seed over a failed load or silently discard recovery material. |
| D3: desired-state idempotence | `Service.Control` / `Decide` durably accept decisions with expected desired revision; return desired state/revision only. A same-desired no-op writes, emits, fences and wakes nothing. START on failed desired RUNNING is a no-op; repair requires RESTART, which the gateway issues itself for a managed Modbus driver. | Reuse refusal ordering, transition slots and bounded reconciliation. Add no lifecycle receipt, client key, completed-operation record or new request member. Caller cancellation after acceptance does not retract durable intent. |
| D5: recovery | `Service.Start` restores committed material through `RecoveryAdapter` before executor calls. `Recorder` implements managed write-ahead restoration and `Secure`; terminal slots are pruned only after recovery material is committed. | Fail affected drivers closed on failed/unknown restoration. Resume pending work; settle historical ACTIVE correlations before new-epoch work; finish pending cleanup. Never reopen merely because desired state says RUNNING. Preserve unrelated valid state and native evidence. |

Disabled configuration wins over durable desired state without rewriting it.
Keep the configured entry visible as disabled; do not construct/open, start
workers, subscribe or publish from registration, list or get. Disabled entries
still undergo descriptor validation and need a usable adapter. Preserve each
native configuration validator: for example, the Growatt starter rejects a
disabled configuration containing active fields. A disabled starter returning
nil is not an admitted runtime; `NewAdapter` rejects nil, so do not call that
starter as a way to implement DISABLED.

The service owns durable decisions and per-driver reconciliation. DriverManager
owns native startup/replacement/close and native retries. Each adapter owns one
generation context, cancel function, all spawned work, consumer leases and join
proof. Shell lifetime is independent. On STOP, fence generation effects before
the publication withdrawal fence, cancel and join helpers, revoke consumer
admission and settle both cleanup obligations. Do not wait for native close
before making semantic withdrawal visible, or vice versa. A pending join stays
DRAINING and blocks successor admission. Native close unconfirmed means
`FAILED/CLOSE_UNCONFIRMED`, quarantine and no successor; semantic cleanup still
pending remains durable. Sending cancel, reaching a deadline or seeing a nil
pointer proves no close.

Use the existing generation-task behavior in
[`ebus_generation_tasks.go`](../../cmd/gateway/ebus_generation_tasks.go) and
`ebusControlExecutor` as the ownership reference, including guarded effect
installation, joined callbacks and late-admission cleanup. These are eBUS
implementations; a sidecar wrapper must supply equivalent ownership without
importing eBUS protocol details into shared runtime code. Generation-key checks
cover driver, source, epoch, driver generation, native mapping and applicable
lifecycle sequence. A stale callback cannot install a provider, change health,
publish or revive a lease, even after a new generation becomes available.

Publication fencing preserves native evidence and partial last-known-good
fields with their provenance and age; it withdraws current admission rather
than replacing retained facts with zeros or treating history as current.
Managed publication must use the hub's per-asset cursor and epoch barriers;
never equate connection generation, driver generation, source epoch, desired
revision or semantic sequence. No direct legacy writer may bypass a migrated
managed writer's fence or recovery path.

Proven: sidecar withdrawal matches driver, source, epoch and generation while
allowing the new STOP or RESTART decision sequence. Active START calls still
require the full admitted correlation; retired generations cannot reopen under
a different sequence. After transport observers join, final eeBUS retirement
uses a terminal revision so earlier positive-revision statuses cannot leave
ready metrics visible during control-plane teardown.

Process exit first closes decision admission, stops reconcilers without a
synthetic durable STOP, fences/joins generation tasks and closes native owners.
It preserves committed desired state and in-flight recovery obligations for
the next incarnation. One sidecar failure must not stop the shell or another
driver's reconciler. Shell status remains separate from driver availability
and native qualification; no status read opens an endpoint.

## Family availability, qualification and composition

| Family | Provider availability (software evidence) | Native qualification | Actual composition after G5 / activation boundary |
| --- | --- | --- | --- |
| SunSpec TCP | Proven: TCP adapter and qualify/refresh path exist. | Requires the existing exact native profile and observation evidence; availability does not qualify a peer. | Proven: managed factory, shared service registration and generation-guarded consumer bindings. |
| Growatt BMS RS-485 | Proven: production provider/open seam exists. | Existing family/revision admission and qualified native evidence remain required. | Proven: inert managed factory and generation-owned lifecycle wrapper exist. Shared service registration and generation-guarded consumer rebinding are composed. Semantic recovery remains subject to the managed-recorder boundary. No Protocol II or CAN inference. |
| eeBUS | Proven: runtime, operator adapter and slot exist. | Existing trust, pairing and native use-case evidence remain independent of process health. | Proven: shared service registration and generation-guarded runtime, operator and capture bindings. Native qualification and semantic promotion remain separate. |
| Huawei SmartLogger | Admitted native read-only provider composed per configured instance. | Native identity, catalogue and child-profile admission remain required; physical qualification is Unknown. | One source per instance; lazily declared PV assets per admitted inverter. No cross-family fallback. |
| Huawei EMMA | Identity-only supplied-evidence seam exists. | No admitted acquisition profile. | Withheld as a registered instance (`NO_ADMITTED_CHILD_PROFILE`); registry issue 199 remains the prerequisite. |
| Huawei S-Dongle | Admitted native read-only provider composed per configured instance. | Native same-connection identity tuple and child profile remain required; no inherited qualification. | One source per instance and one PV asset per admitted child. The native recorded-stop safeguards remain enforced. |
| Growatt Protocol II | Typed/injected-provider contracts exist. | Separate evidence required; BMS RS-485 evidence is insufficient. | Withheld pending its own evidence and decision. |
| OutBack | Typed/injected-provider contracts exist. | Unknown without its own accepted native evidence. | Withheld pending its own evidence and decision. |
| Growatt BMS CAN | Proven: receive-only SocketCAN driver and inert managed factory exist. | Synthetic admission and lifecycle checks do not establish physical qualification or installation compatibility. | Proven: disabled-by-default, host-configured managed receiver with bounded startup retries, correlated failure reporting and cleanup quarantine. No Portal selection controls, public CAN snapshot tools or semantic publication. |
| Other CAN families | Proposed host observation contract; no activation established by this document. | Unknown; native ownership/evidence decisions required. | Withheld. The [CAN observation proposal](can-metric-observation-contract.md) supplies no live route for these families. |

All MCP namespaces remain pre-stable before 1.0. Reuse the five ordinary
`helianthus.runtime.*` tools added by G5; they are shell tools, not descriptor
hooks and not a universal provider SPI. Keep them registered when a driver is
failed/disabled; a missing service answers unavailable. Add no authorization
gate before 0.9 and no namespace here. Native trust and invoke safeguards
remain intact. Existing eBUS envelopes, deterministic hashes/goldens, explicit
invoke safety and executable MCP-to-GraphQL parity remain unchanged.

## Implementation acceptance for M02–M06

Use synthetic/offline fixtures to demonstrate inert registration and disabled
behavior, injected factory failure with shell survival, desired-state no-op
without extra I/O, new-epoch restart, cancel/join before replacement, stale
callback rejection, independent close/semantic cleanup and D5 restore before
open. Identity tests must vary every field of the key they claim to fence and
identify the production mutation caught per field. Preserve native admission
and evidence tests; mock availability is not native qualification.

Public documentation, focused tests, full committed-head CI and independent
review remain implementation acceptance requirements. Production RTU
composition changes also require the declared offline RTU conformance gate;
applicable eBUS transport changes require their transport gate. This document
claims no gate result, physical smoke, installation qualification, parent
closure or release readiness.

## Composed northbound acceptance (M57)

Proven by the `TestM57` fixtures in
[`composition_projection_acceptance_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/composition_projection_acceptance_test.go):
they run the post-G5 lifecycle entrypoint with the production HTTP control
plane, runtime control service, managed registry, process hub, shared snapshot
provider and semantic operation router and kernel. Replaced parts: the eBUS
endpoint is an in-process pipe with its poller and scans stubbed, so its
Thermal value is a synthetic batch published through the admitted
generation's handle; Growatt is the real managed driver and Storage publication
over an in-memory register fake; SunSpec and eeBUS are injected managed
factories, and the SunSpec one publishes synthetic PV batches through its own
hub admission; the control-plane listener is in memory and the owner-only eeBUS
operator socket is an inert closer; the operation route receives a fake intent
owner and executor lookup. The listener, operator-socket and
route-configuration seams (`listenHTTPControlPlane`,
`startEEBusOperatorEndpoint`, `gatewaySemanticOperationConfigFor`) leave
production behavior unchanged, and the M2M and adapter-proxy listeners stay
unconfigured, so no socket is bound.

- Registration, publication and reads agree. Each publication is bound to the
  source its driver registered and to that source's current hub admission.
  GraphQL `semanticSnapshot` and MCP `semantic.v1.snapshot.get` return the same
  publication, revisions, values and losses, including an explicitly withheld
  value. Portal bootstrap advertises the same GraphQL read, and Portal driver
  rows report the lifecycle the MCP list reports. The scrape route serves the
  transport exposition first and withholds semantic samples while no durable
  slot registry exists.
- The ordinary MCP lifecycle tools stop, start and restart composed drivers.
  The composition signals withdrawal before every native stop of an exposed
  generation. A stopped driver reads `source_unavailable` on both surfaces and
  `withheld`/`lifecycle_stopped` in Portal. A publication racing withdrawal and
  any later use of a retired handle are refused, and START after STOP binds a
  new lifecycle epoch.
- A semantic operation completed by an injected executor confirms within one
  generation. Across a RESTART the same completion is never confirmed: the
  SemReg kernel refuses its evidence because the readback names the successor
  epoch. A request naming the retired generation is not sent.
- A sidecar whose construction is refused stays unconstructed. A sidecar whose
  shell lease is refused after its native start is stopped and joined by the
  composition, and its publication is fenced. Both are reported as FAILED by
  the MCP list and as `error` in Portal, while the shell and sibling drivers
  keep serving. Every generation task joins at shutdown.
- After a managed driver restarts, the SemReg kernel admits the successor's
  operation. The `TestM65` fixtures in the same file restart ebus.primary (a
  successor source epoch) and the injected SunSpec factory (a successor driver
  generation of the same epoch) through the ordinary MCP lifecycle tool, with
  the Thermal set_mode route published through each driver's admitted source.
  The shared snapshot keeps the retired route's withdrawn capability beside the
  successor's. The successor's operation is admitted, dispatched on its own
  route and confirmed, while every request naming a retired route member is
  refused before executor lookup. The Growatt Storage publication declares no
  capability, so it is not an operation vehicle. Its successor-generation
  publication is proven by the `TestM61` fixtures described with the Growatt
  lifecycle above.

Not established by these fixtures: operation admission through a production
intent owner, which does not exist yet; the fake owner and executor lookup stand
in for it. Physical transport behavior, installation qualification and release
readiness remain **Unknown**.


### Huawei read-only PV composition

Refs [gateway issue 1363](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1363).
This is the admitted SmartLogger/S-Dongle slice. EMMA acquisition remains blocked
by [the native prerequisite](https://github.com/Project-Helianthus/helianthus-modbusreg/issues/199).
It adds no storage, meter, Portal, control or all-family readiness claim.

The registry owns identity, catalogue selection, native decoding, per-child
backoff, retry bounds and partial last-known-good group readings. The gateway
supplies one connected session exposing only FC03 (at most 123 registers) and
FC2B/MEI 0x0E basic or extended reads. Each identification call requests exactly
one tolerant native page; the provider owns continuation and the next gap.
Configured pacing may lengthen native pacing. It never shortens the native
750 ms SmartLogger gap, 5 s S-Dongle gap or recommended 60 s cycle. A longer
native cycle remains authoritative. Reconnect binds a new session generation
and forces native re-identification before further PV publication.

Asset keys follow the [finalized multi-endpoint contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/modbus-multi-endpoint-runtime.md):
SHA-256 over UTF-8 fields, each followed by NUL: `helianthus.modbus.asset.v1`,
configured instance label, `pv`, routed unit in decimal. Prefix the first 32
lowercase hexadecimal characters with `pv-asset-`. The configured managed
DriverID and owner identity supply the single instance source. Native model,
serial, firmware and endpoint text never select semantic identity. Binding keys
use asset, source and admitted profile as stable material, with the existing
hub epoch/generation minting required for immutable native binding incarnations.
A child that loses qualified identity keeps its native binding `current` with
its identity link withdrawn and no facts or capabilities: the kernel lets a
publisher withdraw only links and facts, and a withdrawn link is never
requalified. A child that later regains admission keeps its configured asset
key but receives a new binding incarnation. Consumers decide whether to act
through the identity link, never through binding state alone.

The [Huawei PV mapping companion](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/mappings/huawei-sun2000-pv-v1.md)
owns decimal mapping, bounds, state vocabulary, input membership and loss.
The gateway consumes typed facts with their native grades and receipt lineage;
it never raises Proven/Hypothesis/Unknown or treats offline mapping qualification
as live qualification. Negative active power, unknown states, zero input pairs,
period counters and unconsumed words remain withheld/native evidence. Child
failures preserve the original group receipt coordinates; a lost identity or
confirmed inventory departure withdraws that child's facts and identity link.
STOP fences the complete instance source before acquisition joins. Successor
publication uses a newer hub generation and refuses the predecessor handle.
Each PV asset keeps retained observations from at most two generations: the
current one and the one before it. When a restart inside the retention window
would admit a third, the admitting batch first withdraws the oldest
generation's retained observations through the registry's fact-withdrawal
path and the instance status lists each withdrawal (asset, source epoch,
generation, count, reason `capacity`). The per-asset retained reservation is
never raised and nothing is removed implicitly.
A retired epoch keeps its source descriptor on every asset it published, and
the semantic kernel bounds an asset at 32 source descriptors. A START whose
new epoch could no longer publish on an asset of the instance source fails
before any epoch is admitted, so the instance never runs without publication.
The native status of an instance withheld for a reason other than its family
(Portal record, descriptor collision, construction failure) is unavailable;
only a family-level withholding reports a closed disposition.
The generic MCP snapshot and GraphQL snapshot share the existing hub; the
SunSpec single-asset outputs keep their existing contracts.

The native status tools require `instance` and expose family, routing, evidence
state, published asset IDs and categorical losses. The EMMA `status.get` reports
its closed refusal; the existing supplied-evidence `identity.get` seam remains
available to its original provider contract. Status reads perform no I/O.
The existing metrics route also emits native wire invocation, response, timeout
and cancellation counters through the endpoint event sink: `helianthus_huawei_transport_events_total`,
aggregated across instances with only closed `family`, `function` and `outcome`
labels. It carries no endpoint, instance, unit, serial or identity labels.

Native protocol authority remains the public
[read pacing contract](https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/huawei/read-pacing-v1.md),
[monitoring contract](https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/huawei/monitoring-readonly-v1.md)
and [gateway contract](https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/huawei/gateway-readonly-v1.md).
Run `bash scripts/huawei_pv_semreg_gate.sh` from a standalone clone. The gate
fetches the pinned public docs-semantic commit carrying the merged identity
companion, refuses the older identity form, executes the public mapping
vectors, checks that the pinned SemReg runtime is selected and runs the
focused gateway, adapter and MCP tests.
