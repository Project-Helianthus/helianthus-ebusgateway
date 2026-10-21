# Managed sidecar composition after G5

Status: application contract for [issue #1290](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1290),
a partial slice of Project #2 INT-06 / INT-07. This specifies subsequent
composition work for M02–M06. The Growatt lifecycle wrapper described below
is implemented for [issue #1305](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1305);
shared service registration and family activation remain subsequent work.

## Contract precedence and evidence

The public [runtime driver lifecycle contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
owns G5 decisions D1, D2, D3 and D5. They are reused unchanged. The local
[runtime/provider contract](runtime-driver-provider-contract-v1.md) records the
implemented seams; its S1/D3 amendment supersedes historical receipt and
idempotency-ledger passages. The
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
an installation. **Hypothesis** describes the proposed managed attachment.
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
manager hooks before startup and registers only `ebus.primary`. Its typed
factory returns the already prepared native runtime wrapper; registry
acceptance alone does not connect the service to a family. There is no
universal sidecar implementation to call today.

## Adapter inputs, outputs and ownership

M02–M06 must attach each family through the existing service, executor and
DriverManager boundaries, rather than introduce another driver manager.
The following are required application bindings, not new exported Go APIs.

| Boundary | Concrete input | Required output and owner |
| --- | --- | --- |
| Composition root | Validated family configuration, configured driver/source identities, exact public native contract reference, injected native factory/open/dial functions, hub, durable store and recovery adapter | Frozen descriptor, constructed wrapper installed as `drivermanager.DriverConfig.Runtime`, and one `runtimecontrol.DriverConfig{DriverID, Executor}` in the same service, in the order below. Configuration and factory dependencies are captured by the family adapter, not encoded in public IDs. Missing identity/dependency withholds that driver. |
| Typed construction | `context.Context`, frozen `Construction` | Family wrapper implementing existing `drivermanager.Runtime`: `Start(ctx) (uint64, error)`, `Replace(ctx) (uint64, error)`, `Stop(ctx) error`, `Generation()`, `Revision()`, `SafetyQuarantined()`. Construction returns an inert wrapper; native opening belongs to its managed start. Clean up partial factory results on failure. |
| Lifecycle execution | `Start(ctx, LifecycleCorrelation)`; correlation binds driver, configured source, epoch, driver generation and lifecycle sequence | Nil only after that generation is admitted. Failure leaves nothing open. Maintain an explicit mapping to native generation; do not cast native counters into semantic generation. Repeated calls for one key join or return its result, without a second start. |
| Withdrawal | `Stop(ctx, LifecycleCorrelation)`, including a generation that never opened | `StopOutcome{NativeClosed, SemanticCleaned}` with independent, truthful obligations. Native close proof comes from `StopWithReport`; semantic cleanup comes from fenced publication withdrawal/epoch retirement. A close error cannot be converted to success because cancellation was sent. |
| Recovery | Existing `RecoveryAdapter.Restore`, `Secure`, and managed `Recorder.Writer(source)` when semantic publication is migrated | Restore before native open/admission; commit secured recovery material before pruning terminal work. Route managed publications through the recorder, not a parallel direct writer. A lifecycle-only adapter cannot claim recovery for semantic assets. |
| Consumer binding | Current admitted generation plus family-owned provider interfaces, Portal contribution leases and publication handles | Stable shell-facing indirection; unavailable while withdrawn. Install generation effects only through a live generation scope and revoke leases before close. Never leave a request holding an unguarded retired provider pointer. |

Required composition order for M02–M06 (Hypothesis until implemented):

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
   runtime registration API; subsequent composition must refactor the current
   primary-only manager construction rather than create a second manager or
   try to append sidecars after it exists.
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
| eeBUS | [`startEEBusRuntime`](../../cmd/gateway/eebus_runtime_adapter.go) takes context, `EEBusConfig`, interface resolver and `eebusRuntimeFactory`; returns `*eebusRuntimeAdapter`. The operator variant returns adapter plus `AdminV1`. Native lifecycle is `Start(ctx)` / `Shutdown()`; adapter provides `Snapshot()` and once-only `Shutdown()`. | Construct and start inside managed ownership. Preserve incomplete-pair cleanup in `startEEBusOperatorRuntime`. Use the existing `eebusRuntimeSlot` reader drain/replace shape and command router; fence readers before draining. Its `Shutdown()` has no context, so prove bounded close/join or report unconfirmed close. Do not turn an eeBUS trust or native write decision into a generic lifecycle decision. |

The existing `eebusRuntimeSlot` methods `BeginRetire`, `DrainRetired`,
`ReplaceDetached`, `shutdownDetached`, `FenceTerminal` and `Shutdown` separate
availability fencing from reader drain and native teardown. They are
eeBUS-local primitives, not a second runtime control service. A detached old
runtime must be shut down and its drain accounted for. A rejected replacement
must also be closed. A slot becoming unavailable proves neither native close
nor semantic cleanup.

## Growatt lifecycle implementation status

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

A stop during construction cancels and joins the pending start. If endpoint open
returns late, startup rechecks cancellation under the same lifecycle lock used
by Stop before exposing the provider. A cancelled start withdraws unpublished
admission (or fences an activated generation) before waiting for native close
and settling. A stop deadline does not prove that open, acquisition or close
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
| D3: desired-state idempotence | `Service.Control` / `Decide` durably accept decisions with expected desired revision; return desired state/revision only. A same-desired no-op writes, emits, fences and wakes nothing. START on failed desired RUNNING is a no-op; repair requires RESTART. | Reuse refusal ordering, transition slots and bounded reconciliation. Add no lifecycle receipt, client key, completed-operation ledger or new request member. Caller cancellation after acceptance does not retract durable intent. |
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

Process exit first closes decision admission, stops reconcilers without a
synthetic durable STOP, fences/joins generation tasks and closes native owners.
It preserves committed desired state and in-flight recovery obligations for
the next incarnation. One sidecar failure must not stop the shell or another
driver's reconciler. Shell status remains separate from driver availability
and native qualification; no status read opens an endpoint.

## Family availability, qualification and composition

| Family | Provider availability (software evidence) | Native qualification | Actual composition after G5 / activation boundary |
| --- | --- | --- | --- |
| SunSpec TCP | Proven: TCP adapter and qualify/refresh path exist. | Requires the existing exact native profile and observation evidence; availability does not qualify a peer. | Existing startup sidecar, not a managed service registration. Managed attachment is Hypothesis until implemented and reviewed. |
| Growatt BMS RS-485 | Proven: production provider/open seam exists. | Existing family/revision admission and qualified native evidence remain required. | Proven: inert managed factory and generation-owned lifecycle wrapper exist. Shared service registration, durable recovery and consumer rebinding remain subsequent work. No Protocol II or CAN inference. |
| eeBUS | Proven: runtime, operator adapter and slot exist. | Existing trust, pairing and native use-case evidence remain independent of process health. | Existing sidecar lifecycle; generic managed registration and semantic admission are not established here. |
| Huawei SmartLogger | Typed/injected-provider contracts exist; availability alone is insufficient. | Unknown for an unprovisioned peer/profile. | Withheld: no activation without its own accepted evidence and composition decision. |
| Huawei EMMA | Typed/injected-provider contracts exist. | Unknown without its own native evidence. | Withheld; SunSpec or SmartLogger evidence cannot activate it. |
| Huawei S-Dongle | Typed/injected-provider contracts exist. | Unknown without its own native evidence. | Withheld; no inherited route or qualification. |
| Growatt Protocol II | Typed/injected-provider contracts exist. | Separate evidence required; BMS RS-485 evidence is insufficient. | Withheld pending its own evidence and decision. |
| OutBack | Typed/injected-provider contracts exist. | Unknown without its own accepted native evidence. | Withheld pending its own evidence and decision. |
| CAN | Proposed host observation contract; no activation established by this document. | Unknown; native ownership/evidence decisions required. | Withheld. The [CAN observation proposal](can-metric-observation-contract.md) supplies no live route. |

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
